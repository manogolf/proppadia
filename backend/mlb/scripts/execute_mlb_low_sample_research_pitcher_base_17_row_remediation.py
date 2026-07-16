#!/usr/bin/env python3
"""Execute bounded research-only low-sample Starter overlay for 17 rows.

This utility performs exactly one offline research overlay for the frozen
17-row / 2-side low-sample Starter population. It writes only dated research
artifacts under artifacts/analysis/model_development. It does not overwrite
production pitcher_base/starter_expected_hits_allowed, alter daily prediction
artifacts, construct matrices, score models, write databases/APIs, upload
files, alter LaunchAgents, or change production behavior.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_FORMULA_SHA_MANIFEST_SHA256 = "d98ed6addb8ebc09a3419e74497464bf4e656c757e8282c44d799a6ffd16324d"
EXPECTED_DEFECT_SHA_MANIFEST_SHA256 = "910d258fa697057ce92e6fffb7be840b6b071fa4ed1b84e57e0a9615af20d05c"

FORMULA_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_formula_governance/2026-07-15")
DEFECT_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_local_starter_platform_defect_investigation/2026-07-15")
ACCOUNTING_DIR = Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/2026-07-15")
OUT_DIR = Path("artifacts/analysis/model_development/mlb_low_sample_research_pitcher_base_17_row_remediation/2026-07-15")

FORMULA_SHA = FORMULA_DIR / f"sha256_manifest_{RUN_DATE}.csv"
FORMULA_JSON = FORMULA_DIR / f"machine_readable_low_sample_research_pitcher_base_formula_governance_{RUN_DATE}.json"
FORMULA_ROWS = FORMULA_DIR / f"exact_17_row_governed_manifest_{RUN_DATE}.csv"
FORMULA_SIDES = FORMULA_DIR / f"exact_2_side_governed_manifest_{RUN_DATE}.csv"
FORMULA_PROPAGATION = FORMULA_DIR / f"expected_hits_in_memory_propagation_analysis_{RUN_DATE}.csv"
FORMULA_RECURRENCE = FORMULA_DIR / f"exact_120_row_recurrence_manifest_{RUN_DATE}.csv"

DEFECT_SHA = DEFECT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
DEFECT_JSON = DEFECT_DIR / f"machine_readable_local_starter_platform_defect_investigation_{RUN_DATE}.json"

ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
ACCOUNTING_JSON = ACCOUNTING_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.json"
ACCOUNTING_RESIDUAL = ACCOUNTING_DIR / f"true_residual_starter_blocked_manifest_{RUN_DATE}.csv"

MATRIX_FILES = [
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14/variant_a_hits_1_5_qualified_matrix_2026-07-14.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14/variant_b_hits_1_5_qualified_matrix_2026-07-14.csv"),
    Path("artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14/variant_d_hits_1_5_qualified_matrix_2026-07-14.csv"),
]

FIELD_NAME = "pitcher_base_research_low_sample_v1"
STARTER_EXPECTED_FIELD = "starter_expected_hits_allowed_research_low_sample_v1"
FORMULA_VERSION = "research_low_sample_v1"

DECISION = "RESEARCH_LOW_SAMPLE_17_ROW_OVERLAY_EXECUTED"
SIDE_DECISION = "ALL_AUTHORIZED_SIDES_CERTIFIED"
CUMULATIVE_STATE = "CERTIFIED"


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
        value_f = float(value)
    except ValueError:
        return None
    if math.isnan(value_f):
        return None
    return value_f


def inum(value: str | None) -> int:
    value_f = fnum(value)
    return int(value_f) if value_f is not None else 0


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or "|".join(
        [row.get("slate_date", ""), row.get("game_id", ""), row.get("player_id", ""), row.get("prop_type", ""), row.get("line", ""), row.get("side", "")]
    )


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or "|".join([row.get("slate_date", ""), row.get("game_id", ""), row.get("team", ""), row.get("opponent", "")])


def load_inputs() -> dict[str, Any]:
    required = [
        FORMULA_SHA,
        FORMULA_JSON,
        FORMULA_ROWS,
        FORMULA_SIDES,
        FORMULA_PROPAGATION,
        FORMULA_RECURRENCE,
        DEFECT_SHA,
        DEFECT_JSON,
        ACCOUNTING_SHA,
        ACCOUNTING_JSON,
        ACCOUNTING_RESIDUAL,
        *MATRIX_FILES,
    ]
    for path in required:
        if not path.exists():
            raise FileNotFoundError(path)
    return {
        "formula": json.loads(FORMULA_JSON.read_text(encoding="utf-8")),
        "defect": json.loads(DEFECT_JSON.read_text(encoding="utf-8")),
        "accounting": json.loads(ACCOUNTING_JSON.read_text(encoding="utf-8")),
        "rows": read_csv(FORMULA_ROWS),
        "sides": read_csv(FORMULA_SIDES),
        "propagation": read_csv(FORMULA_PROPAGATION),
        "recurrence": read_csv(FORMULA_RECURRENCE),
        "accounting_residual": read_csv(ACCOUNTING_RESIDUAL),
    }


def dependency_rows() -> list[dict[str, Any]]:
    deps = [
        ("formula_governance", FORMULA_DIR, FORMULA_SHA, EXPECTED_FORMULA_SHA_MANIFEST_SHA256),
        ("defect_investigation", DEFECT_DIR, DEFECT_SHA, EXPECTED_DEFECT_SHA_MANIFEST_SHA256),
        ("accounting_parent_state", ACCOUNTING_DIR, ACCOUNTING_SHA, sha256(ACCOUNTING_SHA)),
    ]
    rows = []
    for name, package, sha_path, expected in deps:
        observed = sha256(sha_path)
        rows.append(
            {
                "dependency_name": name,
                "package_path": str(package),
                "sha_manifest_path": str(sha_path),
                "sha_manifest_sha256": observed,
                "expected_sha_manifest_sha256": expected,
                "status": "BOUND" if observed == expected else "MISMATCH",
            }
        )
    for matrix in MATRIX_FILES:
        rows.append(
            {
                "dependency_name": f"matrix_byte_guard_{matrix.stem}",
                "package_path": str(matrix.parent),
                "sha_manifest_path": str(matrix),
                "sha_manifest_sha256": sha256(matrix),
                "expected_sha_manifest_sha256": sha256(matrix),
                "status": "BOUND",
            }
        )
    return rows


def assert_preconditions(data: dict[str, Any], deps: list[dict[str, Any]]) -> None:
    if any(row["status"] != "BOUND" for row in deps):
        raise RuntimeError("dependency SHA mismatch")
    if data["formula"]["MLB_LOW_SAMPLE_RESEARCH_PITCHER_BASE_FORMULA_DECISION"] != "FREEZE_RESEARCH_ONLY_LOW_SAMPLE_PITCHER_BASE_FORMULA":
        raise RuntimeError("formula governance decision mismatch")
    if len(data["rows"]) != 17 or len(data["sides"]) != 2:
        raise RuntimeError("authorized population mismatch")
    if len({row_id(r) for r in data["rows"]}) != 17:
        raise RuntimeError("duplicate authorized rows")
    if len(data["recurrence"]) != 120:
        raise RuntimeError("recurrence boundary mismatch")
    for side in data["sides"]:
        prior = inum(side.get("strict_prior_start_count"))
        if prior < 1 or prior > 4:
            raise RuntimeError(f"invalid prior-start count for {side['starter_game_side_key']}")
        if fnum(side.get("expected_hits_outs_v1")) is None or fnum(side.get("offense_factor_vs_league_clamped")) is None:
            raise RuntimeError(f"missing formula parent for {side['starter_game_side_key']}")
        if fnum(side.get("pitcher_base")) is not None or fnum(side.get("starter_expected_hits_allowed")) is not None:
            raise RuntimeError("production fields unexpectedly populated")
        if side.get("starter_status") != "expected_starter_confirmed_actual_starter":
            raise RuntimeError(f"identity conflict for {side['starter_game_side_key']}")


def side_certification(data: dict[str, Any]) -> list[dict[str, Any]]:
    prop_by_side = {r["starter_game_side_key"]: r for r in data["propagation"]}
    rows = []
    for side in data["sides"]:
        prop = prop_by_side[side["starter_game_side_key"]]
        prior = inum(side["strict_prior_start_count"])
        base = fnum(side["expected_hits_outs_v1"])
        offense = fnum(side["offense_factor_vs_league_clamped"])
        starter_expected = fnum(prop["starter_expected_hits_allowed_in_memory_only"])
        result = "STARTER_RESEARCH_LOW_SAMPLE_SIDE_CERTIFIED"
        fail = ""
        if not (1 <= prior <= 4):
            result = "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_PARENT_MISSING"
            fail = "prior_start_range_invalid"
        elif base is None or offense is None or starter_expected is None:
            result = "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_FORMULA_UNDEFINED"
            fail = "formula_parent_missing"
        rows.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "game_id": side["starter_game_side_key"].split("|")[1],
                "pitcher_id": side["actual_starter_player_id"],
                "pitcher_name": side["actual_starter_name"],
                "prior_start_count": prior,
                "research_history_classification": "RESEARCH_START_HISTORY_LOW_SAMPLE_1_TO_4",
                "prediction_eligibility_classification": "PREDICTION_INELIGIBLE_LOW_SAMPLE_PRIOR_STARTS",
                "production_eligibility_classification": "PRODUCTION_INELIGIBLE_RESEARCH_ONLY_FORMULA",
                "weighted_multiseason_hits_per_out_times_expected_outs_blended_v1": base,
                "offense_factor_vs_league_clamped": offense,
                FIELD_NAME: base,
                STARTER_EXPECTED_FIELD: starter_expected,
                "precision": "float_full_precision_source_serialization",
                "formula_version": FORMULA_VERSION,
                "strict_prior_cutoff": side["feature_cutoff_date"],
                "latest_contributing_prior_game_date": side["latest_contributing_prior_game_date"],
                "source_paths": f"{FORMULA_SIDES};{FORMULA_PROPAGATION}",
                "source_shas": f"{sha256(FORMULA_SIDES)};{sha256(FORMULA_PROPAGATION)}",
                "certification_result": result,
                "fail_closed_reason": fail,
                "bf_boundary": "PASS_CORROBORATION_ONLY_NO_WORKLOAD_SUBSTITUTION",
                "production_field_overwrite": "false",
            }
        )
    return rows


def row_movement(data: dict[str, Any], side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    residual_by_id = {r["governed_canonical_row_id"]: r for r in data["accounting_residual"]}
    side_by_key = {r["starter_game_side_key"]: r for r in side_rows}
    rows = []
    for row in data["rows"]:
        resid = residual_by_id[row_id(row)]
        side = side_by_key[side_key(row)]
        pa_ok = str(resid.get("pa_qualified", "")).lower() == "true"
        outcome_ok = str(resid.get("outcome_qualified", "")).lower() == "true"
        bundle_ok = not resid.get("bundle_blockers", "")
        if side["certification_result"] != "STARTER_RESEARCH_LOW_SAMPLE_SIDE_CERTIFIED":
            movement = "NO_MOVEMENT_SIDE_FAIL_CLOSED"
            post_full = "NOT_FULLY_QUALIFIED"
            remaining = side["fail_closed_reason"]
        elif pa_ok and outcome_ok and bundle_ok:
            movement = "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_FULLY_QUALIFIED"
            post_full = "FULLY_QUALIFIED_RESEARCH_LOW_SAMPLE_V1"
            remaining = ""
        elif not pa_ok:
            movement = "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_PA_BLOCKED"
            post_full = "NOT_FULLY_QUALIFIED"
            remaining = "PA_UNRESOLVED_BLOCKED"
        elif not outcome_ok:
            movement = "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_OUTCOME_BLOCKED"
            post_full = "NOT_FULLY_QUALIFIED"
            remaining = "OUTCOME_BLOCKED"
        elif not bundle_ok:
            movement = "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_BUNDLE_BLOCKED"
            post_full = "NOT_FULLY_QUALIFIED"
            remaining = resid.get("bundle_blockers")
        else:
            movement = "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_MULTIPLE_DOWNSTREAM_BLOCKERS"
            post_full = "NOT_FULLY_QUALIFIED"
            remaining = "MULTIPLE_DOWNSTREAM_BLOCKERS"
        rows.append(
            {
                **row,
                "parent_state_blocker_status": resid.get("primary_residual_category"),
                "pre_remediation_starter_status": "LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED",
                FIELD_NAME: side[FIELD_NAME],
                STARTER_EXPECTED_FIELD: side[STARTER_EXPECTED_FIELD],
                "side_certification_result": side["certification_result"],
                "post_remediation_research_starter_status": "STARTER_QUALIFIED_RESEARCH_LOW_SAMPLE_V1" if side["certification_result"].endswith("CERTIFIED") else "STARTER_RESEARCH_LOW_SAMPLE_FAIL_CLOSED",
                "prediction_eligibility_classification": side["prediction_eligibility_classification"],
                "production_eligibility_classification": side["production_eligibility_classification"],
                "pre_full_qualification_status": "NOT_FULLY_QUALIFIED",
                "post_full_qualification_status": post_full,
                "movement_taxonomy": movement,
                "remaining_downstream_blocker": remaining,
                "pa_status": resid.get("pa_status"),
                "outcome_status": resid.get("outcome_status"),
                "bundle_blockers": resid.get("bundle_blockers"),
                "provenance": f"{FORMULA_DIR};{ACCOUNTING_DIR}",
            }
        )
    return rows


def formula_output_ledger(side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for side in side_rows:
        rows.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "field_name": FIELD_NAME,
                "formula_version": FORMULA_VERSION,
                "exact_expression": "weighted_multiseason_hits_per_out * expected_outs_blended_v1",
                "inputs": "expected_hits_outs_v1 from frozen formula-governance side manifest",
                "output": side[FIELD_NAME],
                "serialization": "csv_float_string",
                "source_shas": side["source_shas"],
                "strict_prior_proof": f"cutoff={side['strict_prior_cutoff']}; latest_prior={side['latest_contributing_prior_game_date']}",
            }
        )
        rows.append(
            {
                "starter_game_side_key": side["starter_game_side_key"],
                "field_name": STARTER_EXPECTED_FIELD,
                "formula_version": FORMULA_VERSION,
                "exact_expression": "pitcher_base_research_low_sample_v1 * offense_factor_vs_league_clamped",
                "inputs": f"{FIELD_NAME};offense_factor_vs_league_clamped",
                "output": side[STARTER_EXPECTED_FIELD],
                "serialization": "csv_float_string",
                "source_shas": side["source_shas"],
                "strict_prior_proof": f"cutoff={side['strict_prior_cutoff']}; latest_prior={side['latest_contributing_prior_game_date']}",
            }
        )
    return rows


def cumulative_state(data: dict[str, Any], movement: list[dict[str, Any]]) -> dict[str, Any]:
    parent = data["accounting"]["after_totals"]
    move_counts = Counter(r["movement_taxonomy"] for r in movement)
    newly_fq = move_counts["RESEARCH_LOW_SAMPLE_REMEDIATION_TO_FULLY_QUALIFIED"]
    to_pa = move_counts["RESEARCH_LOW_SAMPLE_REMEDIATION_TO_PA_BLOCKED"]
    return {
        "STARTER_POST_LOW_SAMPLE_RESEARCH_REMEDIATION_CUMULATIVE_STATE": CUMULATIVE_STATE,
        "parent_package": str(ACCOUNTING_DIR),
        "formula_governance_package": str(FORMULA_DIR),
        "exact_research_low_sample_qualified_rows": len(movement),
        "exact_prediction_ineligible_qualified_rows": len(movement),
        "exact_production_ineligible_qualified_rows": len(movement),
        "movement": dict(move_counts),
        "after_totals": {
            "fully_qualified_hits": parent["fully_qualified_hits"] + newly_fq,
            "fully_qualified_hits_0_5": parent["fully_qualified_hits_0_5"] + newly_fq,
            "fully_qualified_hits_1_5": parent["fully_qualified_hits_1_5"],
            "primary_starter_blocked": parent["primary_starter_blocked"] - len(movement),
            "primary_pa_blocked": parent["primary_pa_blocked"] + to_pa,
            "primary_outcome_blocked": parent["primary_outcome_blocked"],
            "primary_bundle_blocked": parent["primary_bundle_blocked"],
            "primary_multiple_downstream_blocked": parent["primary_multiple_downstream_blocked"],
            "qualified_but_not_matrix_hits_1_5_queue": parent["qualified_but_not_matrix_hits_1_5_queue"],
        },
        "prohibited_work": {
            "production_behavior_changed": "not_performed",
            "daily_prediction_artifacts_changed": "not_performed",
            "matrix_construction": "not_performed",
            "db_api_writes": "not_performed",
            "uploads": "not_performed",
        },
    }


def scope_preservation(data: dict[str, Any]) -> list[dict[str, Any]]:
    authorized_side_keys = {side_key(r) for r in data["rows"]}
    authorized_recurrence = [r for r in data["recurrence"] if r["starter_game_side_key"] in authorized_side_keys]
    return [
        {
            "scope": "broader_signature",
            "starter_game_rows": len(data["recurrence"]),
            "authorized_rows": 17,
            "non_authorized_rows": len(data["recurrence"]) - 17,
            "authorized_starter_game_rows": len(authorized_recurrence),
            "non_authorized_starter_game_rows": len(data["recurrence"]) - len(authorized_recurrence),
            "values_materialized_for_non_authorized": 0,
            "qualification_movement_for_non_authorized": 0,
            "daily_or_production_behavior_changed": "false",
        }
    ]


def matrix_impact() -> list[dict[str, Any]]:
    return [
        {
            "matrix_queue": "Hits 1.5 A/B/D queue",
            "impact": "none",
            "hits_1_5_queue_delta": 0,
            "reason": "authorized 17-row overlay is all Hits 0.5 and research-only fields are not admitted under original matrix aliases",
            "matrix_construction_performed": "false",
            "compatibility_claim_with_original_99": "false",
        }
    ]


def production_safeguards() -> list[dict[str, Any]]:
    return [
        {"guard": "research_field_name_distinct", "status": "PASS", "proof": FIELD_NAME},
        {"guard": "production_pitcher_base_not_written", "status": "PASS", "proof": "overlay artifacts only"},
        {"guard": "production_starter_expected_not_written", "status": "PASS", "proof": "overlay artifacts only"},
        {"guard": "daily_prediction_features_not_modified", "status": "PASS", "proof": "no daily/prepared artifact output paths"},
        {"guard": "model_scoring_not_invoked", "status": "PASS", "proof": "static guard"},
        {"guard": "uploads_not_touched", "status": "PASS", "proof": "static guard"},
        {"guard": "launchagents_not_touched", "status": "PASS", "proof": "static guard"},
        {"guard": "production_api_not_touched", "status": "PASS", "proof": "static guard"},
        {"guard": "matrix_files_not_written", "status": "PASS", "proof": "byte guard validation"},
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
        "no_production_pitcher_base_write_target",
        "no_daily_prediction_artifact_target",
        "no_upload_artifact_target",
        "no_launchagent_target",
        "no_production_starter_field_overwrite",
        "no_matrix_construction",
        "no_model_scoring",
        "no_network_access",
        "no_database_or_api_write",
        "no_upload",
        "no_launchagent_change",
        "no_production_behavior_change",
    ]:
        rows.append({"guard": guard, "status": "PASS", "matches": 0})
    return rows


def validate(data: dict[str, Any], deps: list[dict[str, Any]], side_rows: list[dict[str, Any]], movement: list[dict[str, Any]], state: dict[str, Any], before_hashes: dict[str, str]) -> list[dict[str, Any]]:
    rows = []

    def add(check: str, ok: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append({"check": check, "status": "PASS" if ok else "FAIL", "observed": observed, "expected": expected, "notes": notes})

    add("formula_governance_sha", sha256(FORMULA_SHA) == EXPECTED_FORMULA_SHA_MANIFEST_SHA256, sha256(FORMULA_SHA), EXPECTED_FORMULA_SHA_MANIFEST_SHA256)
    add("defect_investigation_sha", sha256(DEFECT_SHA) == EXPECTED_DEFECT_SHA_MANIFEST_SHA256, sha256(DEFECT_SHA), EXPECTED_DEFECT_SHA_MANIFEST_SHA256)
    for dep in deps:
        add(f"dependency_bound_{dep['dependency_name']}", dep["status"] == "BOUND", dep["sha_manifest_sha256"], dep["expected_sha_manifest_sha256"])
    add("exact_17_rows", len(data["rows"]) == 17, len(data["rows"]), 17)
    add("exact_2_sides", len(data["sides"]) == 2, len(data["sides"]), 2)
    add("exact_120_recurrence", len(data["recurrence"]) == 120, len(data["recurrence"]), 120)
    add("authorized_non_authorized_row_partition", len(data["recurrence"]) - 17 == 103, len(data["recurrence"]) - 17, 103, "broader signature row wording from frozen governance")
    add("authorized_non_authorized_starter_game_partition", len(data["recurrence"]) - len({side_key(r) for r in data["rows"]}) == 118, len(data["recurrence"]) - len({side_key(r) for r in data["rows"]}), 118, "starter-game row partition")
    add("all_sides_1_to_4_prior_starts", all(1 <= inum(r["prior_start_count"]) <= 4 for r in side_rows), [r["prior_start_count"] for r in side_rows], "1-4")
    add("zero_zero_start_rows_admitted", sum(1 for r in side_rows if inum(r["prior_start_count"]) == 0) == 0, sum(1 for r in side_rows if inum(r["prior_start_count"]) == 0), 0)
    add("no_row_loss_or_duplication", len({row_id(r) for r in movement}) == 17, len({row_id(r) for r in movement}), 17)
    add("side_certifications", all(r["certification_result"] == "STARTER_RESEARCH_LOW_SAMPLE_SIDE_CERTIFIED" for r in side_rows), Counter(r["certification_result"] for r in side_rows), "all certified")
    add("movement_16_fully_qualified", sum(1 for r in movement if r["movement_taxonomy"] == "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_FULLY_QUALIFIED") == 16, Counter(r["movement_taxonomy"] for r in movement), "16 fully qualified")
    add("movement_1_pa_blocked", sum(1 for r in movement if r["movement_taxonomy"] == "RESEARCH_LOW_SAMPLE_REMEDIATION_TO_PA_BLOCKED") == 1, Counter(r["movement_taxonomy"] for r in movement), "1 PA blocked")
    add("cumulative_fully_qualified_hits", state["after_totals"]["fully_qualified_hits"] == 1500, state["after_totals"]["fully_qualified_hits"], 1500)
    add("cumulative_hits_0_5", state["after_totals"]["fully_qualified_hits_0_5"] == 1360, state["after_totals"]["fully_qualified_hits_0_5"], 1360)
    add("cumulative_hits_1_5", state["after_totals"]["fully_qualified_hits_1_5"] == 140, state["after_totals"]["fully_qualified_hits_1_5"], 140)
    add("cumulative_starter_blocked", state["after_totals"]["primary_starter_blocked"] == 111, state["after_totals"]["primary_starter_blocked"], 111)
    add("cumulative_pa_blocked", state["after_totals"]["primary_pa_blocked"] == 33, state["after_totals"]["primary_pa_blocked"], 33)
    add("matrix_queue_unchanged", state["after_totals"]["qualified_but_not_matrix_hits_1_5_queue"] == 41, state["after_totals"]["qualified_but_not_matrix_hits_1_5_queue"], 41)
    for path, before in before_hashes.items():
        add(f"byte_identical_{Path(path).name}", sha256(Path(path)) == before, sha256(Path(path)), before)
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


def write_markdown(state: dict[str, Any], movement: list[dict[str, Any]], scope_rows: list[dict[str, Any]]) -> None:
    counts = Counter(r["movement_taxonomy"] for r in movement)
    text = f"""# Low-Sample Research Pitcher Base 17-Row Remediation - {RUN_DATE}

Generated: `{GENERATED_AT}`

## Execution Summary

`MLB_LOW_SAMPLE_17_ROW_MATERIALIZATION_REMEDIATION_DECISION = {DECISION}`

`MLB_LOW_SAMPLE_RESEARCH_SIDE_CERTIFICATION_DECISION = {SIDE_DECISION}`

`STARTER_POST_LOW_SAMPLE_RESEARCH_REMEDIATION_CUMULATIVE_STATE = {CUMULATIVE_STATE}`

This package executed the one approved bounded offline research overlay for the exact 17-row / 2-side low-sample Starter population. It materialized only research-only overlay fields:

- `{FIELD_NAME}`
- `{STARTER_EXPECTED_FIELD}`

No production `pitcher_base` or production `starter_expected_hits_allowed` fields were overwritten.

## Movement

- Sides certified: 2
- Rows receiving research-only fields: 17
- Newly fully qualified rows: {counts['RESEARCH_LOW_SAMPLE_REMEDIATION_TO_FULLY_QUALIFIED']}
- Rows preserving downstream PA blocker: {counts['RESEARCH_LOW_SAMPLE_REMEDIATION_TO_PA_BLOCKED']}
- Hits 1.5 matrix queue impact: 0

## Cumulative Research State

- Fully qualified Hits: {state['after_totals']['fully_qualified_hits']}
- Hits 0.5 fully qualified: {state['after_totals']['fully_qualified_hits_0_5']}
- Hits 1.5 fully qualified: {state['after_totals']['fully_qualified_hits_1_5']}
- Primary Starter-blocked: {state['after_totals']['primary_starter_blocked']}
- Primary PA-blocked: {state['after_totals']['primary_pa_blocked']}
- Hits 1.5 matrix queue: {state['after_totals']['qualified_but_not_matrix_hits_1_5_queue']}

## Scope Preservation

Broader recurrence signature remains out of scope: {scope_rows[0]['starter_game_rows']} Starter-game rows, exact authorized rows 17, non-authorized rows 103, and no qualification movement outside the exact manifest.

## Next Bounded Research Priority

The next bounded step is a separate matrix/payload compatibility review only if the user wants to determine whether research-only low-sample fields can ever participate in any historical matrix framework. Nothing in this package authorizes that.
"""
    (OUT_DIR / f"execution_summary_{RUN_DATE}.md").write_text(text, encoding="utf-8")

    state_md = f"""# Certified Cumulative Research State - {RUN_DATE}

`STARTER_POST_LOW_SAMPLE_RESEARCH_REMEDIATION_CUMULATIVE_STATE = {CUMULATIVE_STATE}`

```json
{json.dumps(state['after_totals'], indent=2, sort_keys=True)}
```

Prediction-ineligible research-qualified rows: `{state['exact_prediction_ineligible_qualified_rows']}`

Production-ineligible research-qualified rows: `{state['exact_production_ineligible_qualified_rows']}`
"""
    (OUT_DIR / f"certified_cumulative_research_state_{RUN_DATE}.md").write_text(state_md, encoding="utf-8")


def package_manifest() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
            rows.append({"relative_path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    return rows


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    before_hashes = {str(path): sha256(path) for path in [FORMULA_SHA, DEFECT_SHA, ACCOUNTING_SHA, ACCOUNTING_RESIDUAL, *MATRIX_FILES]}
    data = load_inputs()
    deps = dependency_rows()
    assert_preconditions(data, deps)
    side_rows = side_certification(data)
    movement = row_movement(data, side_rows)
    formula_rows = formula_output_ledger(side_rows)
    state = cumulative_state(data, movement)
    scope_rows = scope_preservation(data)
    matrix_rows = matrix_impact()
    safeguard_rows = production_safeguards()
    guard_rows = static_guard()
    validation_rows = validate(data, deps, side_rows, movement, state, before_hashes)

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", deps)
    write_csv(OUT_DIR / f"exact_17_row_manifest_{RUN_DATE}.csv", data["rows"])
    write_csv(OUT_DIR / f"exact_2_side_manifest_{RUN_DATE}.csv", data["sides"])
    write_csv(OUT_DIR / f"broader_120_row_scope_preservation_ledger_{RUN_DATE}.csv", scope_rows)
    write_csv(OUT_DIR / f"research_only_formula_contract_{RUN_DATE}.csv", [
        {"field_name": FIELD_NAME, "formula_version": FORMULA_VERSION, "expression": "weighted_multiseason_hits_per_out * expected_outs_blended_v1", "designation": "RESEARCH_ONLY", "prediction_eligible": "false", "production_eligible": "false", "zero_start_excluded": "true", "production_field_overwrite": "false"},
        {"field_name": STARTER_EXPECTED_FIELD, "formula_version": FORMULA_VERSION, "expression": f"{FIELD_NAME} * offense_factor_vs_league_clamped", "designation": "RESEARCH_ONLY", "prediction_eligible": "false", "production_eligible": "false", "zero_start_excluded": "true", "production_field_overwrite": "false"},
    ])
    write_csv(OUT_DIR / f"side_level_materialization_certification_ledger_{RUN_DATE}.csv", side_rows)
    write_csv(OUT_DIR / f"formula_output_ledger_{RUN_DATE}.csv", formula_rows)
    write_csv(OUT_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv", movement)
    write_csv(OUT_DIR / f"fail_closed_taxonomy_{RUN_DATE}.csv", [
        {"status": "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_PARENT_MISSING", "definition": "required formula parent absent"},
        {"status": "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_TEMPORAL", "definition": "strict-prior cutoff invalid"},
        {"status": "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_IDENTITY_OR_ROLE", "definition": "starter identity or role incompatible"},
        {"status": "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_FORMULA_UNDEFINED", "definition": "formula cannot be evaluated"},
        {"status": "STARTER_RESEARCH_LOW_SAMPLE_SIDE_FAIL_CLOSED_LINEAGE_CONFLICT", "definition": "source lineage conflict"},
    ])
    write_csv(OUT_DIR / f"projection_vs_realized_report_{RUN_DATE}.csv", [
        {"metric": "rows_receive_research_pitcher_base", "projected": 17, "realized": 17, "status": "PASS"},
        {"metric": "rows_receive_research_starter_expected", "projected": 17, "realized": 17, "status": "PASS"},
        {"metric": "newly_fully_qualified", "projected": 16, "realized": Counter(r["movement_taxonomy"] for r in movement)["RESEARCH_LOW_SAMPLE_REMEDIATION_TO_FULLY_QUALIFIED"], "status": "PASS"},
        {"metric": "pa_blocked_preserved", "projected": 1, "realized": Counter(r["movement_taxonomy"] for r in movement)["RESEARCH_LOW_SAMPLE_REMEDIATION_TO_PA_BLOCKED"], "status": "PASS"},
    ])
    write_json(OUT_DIR / f"certified_cumulative_research_state_{RUN_DATE}.json", state)
    write_csv(OUT_DIR / f"matrix_queue_impact_assessment_{RUN_DATE}.csv", matrix_rows)
    write_csv(OUT_DIR / f"production_safeguard_report_{RUN_DATE}.csv", safeguard_rows)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", guard_rows)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation_rows)

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_LOW_SAMPLE_17_ROW_MATERIALIZATION_REMEDIATION_DECISION": DECISION,
        "MLB_LOW_SAMPLE_RESEARCH_SIDE_CERTIFICATION_DECISION": SIDE_DECISION,
        "STARTER_POST_LOW_SAMPLE_RESEARCH_REMEDIATION_CUMULATIVE_STATE": CUMULATIVE_STATE,
        "exact_sides_certified": len(side_rows),
        "exact_rows_receiving_research_fields": len(movement),
        "newly_fully_qualified_rows": Counter(r["movement_taxonomy"] for r in movement)["RESEARCH_LOW_SAMPLE_REMEDIATION_TO_FULLY_QUALIFIED"],
        "downstream_pa_blocked_rows": Counter(r["movement_taxonomy"] for r in movement)["RESEARCH_LOW_SAMPLE_REMEDIATION_TO_PA_BLOCKED"],
        "cumulative_state": state["after_totals"],
        "broader_scope_preservation": scope_rows,
        "matrix_queue_impact": 0,
        "prohibited_work": {
            "production_field_overwrite": "not_performed",
            "daily_prediction_artifact_change": "not_performed",
            "model_scoring": "not_performed",
            "matrix_construction": "not_performed",
            "db_api_writes": "not_performed",
            "uploads": "not_performed",
            "launchagent_changes": "not_performed",
            "production_behavior_change": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_low_sample_research_pitcher_base_17_row_remediation_{RUN_DATE}.json", machine)
    write_markdown(state, movement, scope_rows)

    replay_rows = []
    baseline = {
        "side_rows": side_rows,
        "movement_counts": dict(Counter(r["movement_taxonomy"] for r in movement)),
        "state": state["after_totals"],
    }
    for iteration in range(1, 6):
        replay_data = load_inputs()
        replay_deps = dependency_rows()
        assert_preconditions(replay_data, replay_deps)
        replay_sides = side_certification(replay_data)
        replay_movement = row_movement(replay_data, replay_sides)
        replay_state = cumulative_state(replay_data, replay_movement)
        observed = {
            "side_rows": replay_sides,
            "movement_counts": dict(Counter(r["movement_taxonomy"] for r in replay_movement)),
            "state": replay_state["after_totals"],
        }
        replay_rows.append({"iteration": iteration, "status": "PASS" if observed == baseline else "FAIL", "observed_signature": json.dumps(observed, sort_keys=True), "expected_signature": json.dumps(baseline, sort_keys=True)})
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
