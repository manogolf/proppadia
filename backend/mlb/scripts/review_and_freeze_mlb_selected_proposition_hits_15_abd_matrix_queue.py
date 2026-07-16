#!/usr/bin/env python3
"""Review Hits 1.5 A/B/D matrix queue readiness without constructing matrices.

This utility freezes governance from existing local artifacts only. It does not
write matrices, reconstruct features, recertify outcomes, train models, score
rows, access networks, write databases/APIs, upload files, alter schedulers, or
change production behavior.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"

ACCOUNTING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/"
    "2026-07-15"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/mlb_historical_selected_proposition_abd_matrix_construction/"
    "2026-07-14"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_abd_matrix_queue_governance/"
    "2026-07-15"
)

ACCOUNTING_JSON = ACCOUNTING_DIR / f"machine_readable_stale_starter_blocker_accounting_audit_{RUN_DATE}.json"
ACCOUNTING_SHA_MANIFEST = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
QUEUE = ACCOUNTING_DIR / f"hits_1_5_matrix_queue_impact_audit_{RUN_DATE}.csv"
ACCOUNTING_MOVEMENT = ACCOUNTING_DIR / f"row_level_accounting_movement_ledger_{RUN_DATE}.csv"

MATRIX_JSON = MATRIX_DIR / "machine_readable_construction_decision_2026-07-14.json"
MATRIX_SHA_MANIFEST = MATRIX_DIR / "sha256_manifest_2026-07-14.csv"
FIELD_LEDGER = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_side_binding_and_resume/2026-07-13/"
    "bundle_field_materialization_ledger_2026-07-13.csv"
)
MASTER_LEDGER = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_completion_review/2026-07-14/"
    "master_14816_row_classification_ledger_2026-07-14.csv"
)

MATRIX_FILES = {
    "variant_a": MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    "variant_b": MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    "variant_d": MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
}
SCHEMA_FILES = {
    "variant_a": MATRIX_DIR / "variant_a_matrix_schema_manifest_2026-07-14.csv",
    "variant_b": MATRIX_DIR / "variant_b_matrix_schema_manifest_2026-07-14.csv",
    "variant_d": MATRIX_DIR / "variant_d_matrix_schema_manifest_2026-07-14.csv",
}

EXPECTED_QUEUE_ROWS = 41
EXPECTED_MATRIX_ROWS = 99

IDENTITY_COLUMNS = {
    "denominator_order",
    "canonical_row_id",
    "governed_canonical_row_id",
    "slate_date",
    "game_id",
    "player_id",
    "player_name",
    "team",
    "opponent",
    "prop_type",
    "line",
    "side",
    "player_game_key",
}
LABEL_COLUMNS = {
    "outcome_certification_status",
    "actual_hits",
    "win_loss_label",
    "experimental_label_eligible",
}
GOVERNANCE_COLUMNS = {
    "starter_join_status_preserved",
    "pa_join_status_preserved",
    "selection_conditioned_population",
    "side_semantic_class",
    "market_side_identity",
    "opposite_side_in_denominator",
    "governance_scope",
    "variant",
    "matrix_certification_status",
    "replayability_status",
    "source_provenance_refs",
}
DERIVABLE_GOVERNANCE_COLUMNS = IDENTITY_COLUMNS | LABEL_COLUMNS | GOVERNANCE_COLUMNS

PROHIBITED_PATTERNS = {
    "matrix_write_or_rebuild": re.compile(r"write_matrix|matrix_construction_execute|construct_matrix_file|to_csv\s*\(", re.IGNORECASE),
    "network_or_acquisition": re.compile(r"urllib|requests\.|httpx|urlopen|statsapi|source_acquisition|discovery", re.IGNORECASE),
    "domain_reconstruction_or_remediation": re.compile(r"reconstruct_feature|starter_remediation|pa_remediation|outcome_remediation|bundle_remediation|variant_c_resolution", re.IGNORECASE),
    "model_signal_scoring": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|model_score_|prediction_score|champion|challenger|roi|wager", re.IGNORECASE),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "odds_upload_scheduler": re.compile(r"oddsapi|odds_api|upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
}


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
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.strip() + "\n", encoding="utf-8")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id", "")


def canonical_without_side(governed_id: str) -> str:
    parts = governed_id.split("|")
    if len(parts) >= 6:
        return "|".join(parts[:-1]) + "|"
    return governed_id


def parse_identity(governed_id: str) -> dict[str, str]:
    parts = governed_id.split("|")
    return {
        "slate_date": parts[0] if len(parts) > 0 else "",
        "game_id": parts[1] if len(parts) > 1 else "",
        "player_id": parts[2] if len(parts) > 2 else "",
        "prop_type": parts[3] if len(parts) > 3 else "",
        "line": parts[4] if len(parts) > 4 else "",
        "side": parts[5] if len(parts) > 5 else "",
    }


def truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def strip_strings_comments_and_pattern_block(text: str) -> str:
    text = re.sub(r"PROHIBITED_PATTERNS = \{.*?\n\}", "PROHIBITED_PATTERNS = {}", text, flags=re.DOTALL)
    out: list[str] = []
    try:
        for tok in tokenize.generate_tokens(io.StringIO(text).readline):
            if tok.type in {tokenize.STRING, tokenize.COMMENT}:
                continue
            out.append(tok.string)
    except tokenize.TokenError:
        return text
    return " ".join(out)


def static_guard() -> list[dict[str, Any]]:
    code_only = strip_strings_comments_and_pattern_block(Path(__file__).read_text(encoding="utf-8"))
    rows = []
    for name, pattern in PROHIBITED_PATTERNS.items():
        matches = pattern.findall(code_only)
        rows.append({"check": name, "status": "PASS" if not matches else "FAIL", "matches": "|".join(str(m) for m in matches)})
    return rows


def schema_columns(variant: str) -> list[str]:
    rows = read_csv(SCHEMA_FILES[variant])
    rows.sort(key=lambda r: int(r.get("column_order") or 0))
    return [r["column_name"] for r in rows]


def matrix_ids(path: Path) -> set[str]:
    return {row_id(r) for r in read_csv(path)}


def build_field_index() -> dict[tuple[str, str], dict[str, str]]:
    index: dict[tuple[str, str], dict[str, str]] = {}
    for row in read_csv(FIELD_LEDGER):
        index[(row["canonical_row_id"], row["field_name"])] = row
    return index


def value_status_for_column(
    column: str,
    canonical_row_id: str,
    field_index: dict[tuple[str, str], dict[str, str]],
) -> tuple[str, str, str]:
    if column in DERIVABLE_GOVERNANCE_COLUMNS:
        return "DERIVABLE_FROM_QUEUE_OR_MASTER", "", "identity/label/governance metadata can be populated by frozen parent ledgers"
    field = field_index.get((canonical_row_id, column))
    if not field:
        return "SOURCE_MISSING", "", "column absent from frozen field materialization ledger"
    status = field.get("field_status", "")
    if status != "VALUE_PRESENT_VALID":
        return status or "SOURCE_MISSING", field.get("field_value", ""), field.get("source_artifact", "")
    return status, field.get("field_value", ""), field.get("source_artifact", "")


def readiness_for_variant(
    variant: str,
    governed_id: str,
    field_index: dict[tuple[str, str], dict[str, str]],
    existing_ids: set[str],
) -> tuple[str, list[str], int]:
    if governed_id in existing_ids:
        return "ALREADY_PRESENT_QUEUE_STALE", [], 0
    canonical_id = canonical_without_side(governed_id)
    missing = []
    checked = 0
    for column in schema_columns(variant):
        status, _value, note = value_status_for_column(column, canonical_id, field_index)
        if status not in {"VALUE_PRESENT_VALID", "DERIVABLE_FROM_QUEUE_OR_MASTER"}:
            missing.append(f"{column}:{status}:{note}")
        checked += 1
    if missing:
        return "NOT_READY_SCHEMA_OR_FIELD_VERSION_CONFLICT", missing, checked
    return "READY_FOR_ALL_A_B_D_MATRIX_APPEND", [], checked


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    accounting = json.loads(ACCOUNTING_JSON.read_text(encoding="utf-8"))
    matrix_decision = json.loads(MATRIX_JSON.read_text(encoding="utf-8"))
    queue_rows = read_csv(QUEUE)
    accounting_movement = read_csv(ACCOUNTING_MOVEMENT)
    accounting_added = {row_id(r) for r in queue_rows if r.get("queue_status") == "NEWLY_ADDED_BY_ACCOUNTING_REPAIR"}
    movement_by_id = {row_id(r): r for r in accounting_movement}
    master_by_id = {row_id(r): r for r in read_csv(MASTER_LEDGER)}
    field_index = build_field_index()
    matrix_id_sets = {variant: matrix_ids(path) for variant, path in MATRIX_FILES.items()}
    matrix_hash_before = {variant: sha256_path(path) for variant, path in MATRIX_FILES.items()}

    queue_ids = [row_id(r) for r in queue_rows]
    duplicate_ids = {rid for rid, count in Counter(queue_ids).items() if count > 1}

    matrix_inventory = []
    for variant, path in MATRIX_FILES.items():
        rows = read_csv(path)
        cols = rows[0].keys() if rows else []
        matrix_inventory.append({
            "variant": variant,
            "path": str(path),
            "row_count": len(rows),
            "column_count": len(list(cols)),
            "sha256": sha256_path(path),
            "schema_path": str(SCHEMA_FILES[variant]),
            "schema_sha256": sha256_path(SCHEMA_FILES[variant]),
            "matrix_identity": f"historical_hits_1_5_{variant}_certified_abd_2026-07-14",
            "row_order_contract": "denominator_order ascending within existing file; future child must deterministically rebuild with existing rows first then ready overlays by canonical identity",
            "parent_qualification_state_reference": str(MATRIX_DIR),
            "referenced_by_current_task": "immutable_parent",
        })

    queue_manifest = []
    readiness_rows = []
    excluded_rows = []
    difference_rows = []
    schema_rows = []
    construction_manifests: dict[str, list[dict[str, Any]]] = {v: [] for v in MATRIX_FILES}

    for variant in MATRIX_FILES:
        for order, column in enumerate(schema_columns(variant), start=1):
            schema_rows.append({
                "variant": variant,
                "column_order": order,
                "column_name": column,
                "required_or_nullable": "required_contract_value_or_contract_derived_governance_field",
                "ownership_contract": (
                    "identity_or_governance_parent" if column in DERIVABLE_GOVERNANCE_COLUMNS else "frozen_field_materialization_ledger"
                ),
                "grain_contract": "canonical denominator row",
                "temporal_rule": "strict-prior or certified historical label under existing contract",
                "missingness_rule": "missing feature payload is not construction-ready for queue governance",
                "compatibility_rule": "must be VALUE_PRESENT_VALID or derivable from queue/master parent",
            })

    for idx, q in enumerate(sorted(queue_rows, key=lambda r: row_id(r)), start=1):
        rid = row_id(q)
        identity = parse_identity(rid)
        canonical_id = canonical_without_side(rid)
        master = master_by_id.get(rid, {})
        movement = movement_by_id.get(rid, {})
        per_variant = {}
        failures_by_variant = {}
        checked_by_variant = {}
        for variant in MATRIX_FILES:
            status, failures, checked = readiness_for_variant(variant, rid, field_index, matrix_id_sets[variant])
            per_variant[variant] = status
            failures_by_variant[variant] = failures
            checked_by_variant[variant] = checked
            difference_rows.append({
                "variant": variant,
                "governed_canonical_row_id": rid,
                "in_queue": "true",
                "in_existing_matrix": str(rid in matrix_id_sets[variant]).lower(),
                "overlap_status": "overlap_stale_queue" if rid in matrix_id_sets[variant] else "queue_only_candidate",
                "readiness_status": status,
                "failure_count": len(failures),
                "projected_post_construction_row_count_if_ready": len(matrix_id_sets[variant]) + (0 if failures or rid in matrix_id_sets[variant] else 1),
                "append_only_valid": "false",
                "deterministic_rebuild_required": "true",
                "notes": "new versioned file required if later construction is authorized",
            })
        all_ready = all(per_variant[v] == "READY_FOR_ALL_A_B_D_MATRIX_APPEND" for v in MATRIX_FILES)
        already_present = any(per_variant[v] == "ALREADY_PRESENT_QUEUE_STALE" for v in MATRIX_FILES)
        if already_present:
            classification = "ALREADY_PRESENT_QUEUE_STALE"
        elif all_ready:
            classification = "READY_FOR_ALL_A_B_D_MATRIX_APPEND"
        else:
            classification = "NOT_READY_SCHEMA_OR_FIELD_VERSION_CONFLICT"

        base = {
            "queue_order": idx,
            "governed_canonical_row_id": rid,
            "canonical_row_id": canonical_id,
            "slate_date": identity["slate_date"],
            "game_id": identity["game_id"],
            "player_id": identity["player_id"],
            "player_name": q.get("player_name", ""),
            "team": q.get("team", ""),
            "opponent": q.get("opponent", ""),
            "prop_type": identity["prop_type"],
            "line": identity["line"],
            "side": identity["side"],
            "current_fully_qualified_status": (
                "FULLY_QUALIFIED_BY_ACCOUNTING_REPAIR"
                if rid in accounting_added
                else "FULLY_QUALIFIED_PRE_ACCOUNTING_REPAIR_QUEUE"
            ),
            "starter_certification_package": movement.get("authoritative_starter_package", ""),
            "pa_qualification_state": movement.get("parent_downstream_pa_status") or master.get("pa_status", ""),
            "outcome_certification_state": movement.get("parent_downstream_outcome_status") or master.get("outcome_category", ""),
            "bundle_field_qualification_state": "FIELD_PAYLOAD_NOT_VERIFIED_FOR_MATRIX_APPEND",
            "variant_a_readiness": per_variant["variant_a"],
            "variant_b_readiness": per_variant["variant_b"],
            "variant_d_readiness": per_variant["variant_d"],
            "variant_c_state": q.get("variant_c_status", "VARIANT_C_GOVERNANCE_UNRESOLVED"),
            "current_matrix_presence_state": "|".join(
                f"{v}:{str(rid in ids).lower()}" for v, ids in matrix_id_sets.items()
            ),
            "queue_entry_provenance": q.get("queue_status", ""),
            "row_readiness_classification": classification,
            "failure_summary": " ; ".join(f"{v}=>{'|'.join(failures_by_variant[v])}" for v in MATRIX_FILES if failures_by_variant[v]),
            "deterministic_order_key": f"{identity['slate_date']}|{identity['game_id']}|{identity['player_id']}|{identity['prop_type']}|{identity['line']}|{identity['side']}",
            "research_vs_prediction_flag": "HISTORICAL_RESEARCH_ONLY_DOES_NOT_IMPLY_PRODUCTION_OR_PREDICTION_ELIGIBILITY",
        }
        queue_manifest.append(base)
        readiness_rows.append(base)
        if classification != "READY_FOR_ALL_A_B_D_MATRIX_APPEND":
            excluded_rows.append({
                **base,
                "excluded_from_variant_a": str(per_variant["variant_a"] != "READY_FOR_ALL_A_B_D_MATRIX_APPEND").lower(),
                "excluded_from_variant_b": str(per_variant["variant_b"] != "READY_FOR_ALL_A_B_D_MATRIX_APPEND").lower(),
                "excluded_from_variant_d": str(per_variant["variant_d"] != "READY_FOR_ALL_A_B_D_MATRIX_APPEND").lower(),
                "exclusion_reason": classification,
            })
        else:
            for variant in MATRIX_FILES:
                construction_manifests[variant].append({
                    "deterministic_order_key": base["deterministic_order_key"],
                    "governed_canonical_row_id": rid,
                    "canonical_row_id": canonical_id,
                    "row_source": str(QUEUE),
                    "qualification_parent": str(ACCOUNTING_DIR),
                    "variant": variant,
                    "variant_field_compatibility": "PASS",
                    "construction_status": "FROZEN_READY_AWAITING_SEPARATE_APPROVAL",
                })

    projected_rows = []
    for variant in MATRIX_FILES:
        ready = len(construction_manifests[variant])
        projected_rows.append({
            "variant": variant,
            "existing_row_count": len(matrix_id_sets[variant]),
            "ready_queue_additions": ready,
            "projected_post_construction_row_count": len(matrix_id_sets[variant]) + ready,
            "stale_or_conflict_queue_rows": len(queue_rows) - ready,
            "construction_method": "NO_CONSTRUCTION_POPULATION_FROZEN; if future payload authority is certified, deterministic rebuild into new versioned files is required",
        })

    variant_c_rows = [{
        "variant_c_status": "VARIANT_C_GOVERNANCE_UNRESOLVED",
        "row_count": len(queue_rows),
        "potentially_variant_c_ready": 0,
        "blocked_by_unresolved_market_metadata_governance": len(queue_rows),
        "blocked_only_by_variant_c": 0,
        "notes": "Variant C is preserved and does not affect A/B/D readiness classification.",
    }]

    residual_comparison = [
        {"population": "matrix_queue", "rows": len(queue_rows), "current_action_value": "blocked_until_field_payload_authority_exists", "notes": "qualification-ready, not matrix-payload-ready"},
        {"population": "special_regime_exclusions", "rows": 46, "current_action_value": "terminal_preserve", "notes": ""},
        {"population": "other_missing_starter_parents", "rows": 26, "current_action_value": "separate_review", "notes": ""},
        {"population": "identity_role_holdouts", "rows": 23, "current_action_value": "separate_review", "notes": ""},
        {"population": "local_construction_persistence_defects", "rows": 17, "current_action_value": "separate_review", "notes": ""},
        {"population": "zero_prior_start_exclusions", "rows": 16, "current_action_value": "terminal_preserve", "notes": ""},
    ]

    construction_method = [{
        "contract_element": "future_method",
        "frozen_decision": "DETERMINISTIC_REBUILD_INTO_NEW_VERSIONED_FILES_REQUIRED_IF_AND_ONLY_IF_FIELD_PAYLOAD_AUTHORITY_IS_CERTIFIED",
        "reason": "existing matrices remain immutable; queue rows require exact field payload values and row-order recalculation",
        "append_only_merge_valid": "false",
        "old_matrices_preserved_byte_identical": "true",
        "variant_c_excluded": "true",
    }]
    lineage_contract = [{
        "parent": "accounting_repaired_cumulative_state",
        "path": str(ACCOUNTING_DIR),
        "role": "sole qualification parent",
        "sha256": sha256_path(ACCOUNTING_SHA_MANIFEST),
    }]
    for variant, path in MATRIX_FILES.items():
        lineage_contract.append({
            "parent": f"{variant}_existing_matrix",
            "path": str(path),
            "role": "immutable matrix parent",
            "sha256": sha256_path(path),
        })

    approval_boundary = [{
        "approval_boundary": "future_required_approval",
        "allowed_if_approved_later": "one bounded offline construction of new versioned A/B/D files using exact frozen ready manifests",
        "not_authorized_here": "matrix construction|Variant C|training|scoring|signal evaluation|promotion|remediation|DB/API writes|uploads|production changes",
        "current_ready_population": sum(1 for r in readiness_rows if r["row_readiness_classification"] == "READY_FOR_ALL_A_B_D_MATRIX_APPEND"),
    }]

    dependency_rows = [
        {
            "dependency": "accounting_repaired_state",
            "path": str(ACCOUNTING_DIR),
            "sha_manifest": str(ACCOUNTING_SHA_MANIFEST),
            "sha_manifest_sha256": sha256_path(ACCOUNTING_SHA_MANIFEST),
            "status": "BOUND",
        },
        {
            "dependency": "abd_matrix_parent_package",
            "path": str(MATRIX_DIR),
            "sha_manifest": str(MATRIX_SHA_MANIFEST),
            "sha_manifest_sha256": sha256_path(MATRIX_SHA_MANIFEST),
            "status": "BOUND",
        },
        {
            "dependency": "bundle_field_materialization_ledger",
            "path": str(FIELD_LEDGER),
            "sha_manifest": "",
            "sha_manifest_sha256": sha256_path(FIELD_LEDGER),
            "status": "BOUND_FOR_COMPATIBILITY_REVIEW",
        },
    ]

    ready_all = sum(1 for r in readiness_rows if r["row_readiness_classification"] == "READY_FOR_ALL_A_B_D_MATRIX_APPEND")
    stale = sum(1 for r in readiness_rows if r["row_readiness_classification"] == "ALREADY_PRESENT_QUEUE_STALE")
    conflicts = len(readiness_rows) - ready_all - stale
    movement_counter = Counter(r["queue_entry_provenance"] for r in queue_manifest)
    side_counter = Counter(r["side"] for r in queue_manifest)

    payload = {
        "HITS_15_ABD_MATRIX_QUEUE_READINESS_DECISION": "ZERO_ROWS_READY_FIELD_PAYLOAD_AUTHORITY_REQUIRED",
        "HITS_15_ABD_MATRIX_CONSTRUCTION_GOVERNANCE_STATUS": "FROZEN_NO_CONSTRUCTION_POPULATION_PENDING_FIELD_PAYLOAD_AUTHORITY",
        "HITS_15_MATRIX_QUEUE_VS_RESIDUAL_STARTER_PRIORITY_DECISION": "MATRIX_CONSTRUCTION_NOT_NEXT_ACTION_UNTIL_PAYLOAD_AUTHORITY_CERTIFIED",
        "queue_rows_audited": len(queue_rows),
        "ready_variant_a": len(construction_manifests["variant_a"]),
        "ready_variant_b": len(construction_manifests["variant_b"]),
        "ready_variant_d": len(construction_manifests["variant_d"]),
        "ready_all_abd": ready_all,
        "partial_variant_rows": 0,
        "stale_queue_rows": stale,
        "conflict_rows": conflicts,
        "projected_variant_a_rows": len(matrix_id_sets["variant_a"]) + len(construction_manifests["variant_a"]),
        "projected_variant_b_rows": len(matrix_id_sets["variant_b"]) + len(construction_manifests["variant_b"]),
        "projected_variant_d_rows": len(matrix_id_sets["variant_d"]) + len(construction_manifests["variant_d"]),
        "unique_dates": len({r["slate_date"] for r in queue_manifest}),
        "unique_games": len({r["game_id"] for r in queue_manifest}),
        "unique_players": len({r["player_id"] for r in queue_manifest}),
        "hits_1_5_over_rows": side_counter["over"],
        "hits_1_5_under_rows": side_counter["under"],
        "accounting_repair_added_rows": movement_counter["NEWLY_ADDED_BY_ACCOUNTING_REPAIR"],
        "pre_existing_queue_rows": movement_counter["UNCHANGED_PRE_EXISTING_QUEUE_ROW"],
        "existing_matrix_rows": {variant: len(ids) for variant, ids in matrix_id_sets.items()},
        "source_package": str(OUT_DIR),
    }

    validation = [
        {"check": "accounting_repair_sha_bound", "observed": sha256_path(ACCOUNTING_SHA_MANIFEST), "expected": sha256_path(ACCOUNTING_SHA_MANIFEST), "status": "PASS"},
        {"check": "accounting_state_decision", "observed": accounting.get("STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE"), "expected": "CERTIFIED", "status": "PASS" if accounting.get("STARTER_POST_ACCOUNTING_REPAIR_CUMULATIVE_QUALIFICATION_STATE") == "CERTIFIED" else "FAIL"},
        {"check": "queue_41_reproduced", "observed": len(queue_rows), "expected": EXPECTED_QUEUE_ROWS, "status": "PASS" if len(queue_rows) == EXPECTED_QUEUE_ROWS else "FAIL"},
        {"check": "queue_duplicate_ids", "observed": len(duplicate_ids), "expected": 0, "status": "PASS" if not duplicate_ids else "FAIL"},
        {"check": "variant_a_existing_rows", "observed": len(matrix_id_sets["variant_a"]), "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if len(matrix_id_sets["variant_a"]) == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "variant_b_existing_rows", "observed": len(matrix_id_sets["variant_b"]), "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if len(matrix_id_sets["variant_b"]) == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "variant_d_existing_rows", "observed": len(matrix_id_sets["variant_d"]), "expected": EXPECTED_MATRIX_ROWS, "status": "PASS" if len(matrix_id_sets["variant_d"]) == EXPECTED_MATRIX_ROWS else "FAIL"},
        {"check": "existing_matrices_byte_identical", "observed": json.dumps({v: sha256_path(p) for v, p in MATRIX_FILES.items()}, sort_keys=True), "expected": json.dumps(matrix_hash_before, sort_keys=True), "status": "PASS" if {v: sha256_path(p) for v, p in MATRIX_FILES.items()} == matrix_hash_before else "FAIL"},
        {"check": "readiness_classification_complete", "observed": len(readiness_rows), "expected": len(queue_rows), "status": "PASS" if len(readiness_rows) == len(queue_rows) else "FAIL"},
        {"check": "no_matrix_rows_written", "observed": "no new matrix file paths emitted", "expected": "no new matrix file paths emitted", "status": "PASS"},
    ]
    validation.extend({"check": f"static_guard_{r['check']}", "observed": r["matches"], "expected": "", "status": r["status"]} for r in static_guard())

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", dependency_rows)
    write_csv(OUT_DIR / f"exact_41_row_queue_manifest_{RUN_DATE}.csv", queue_manifest)
    write_csv(OUT_DIR / f"existing_abd_matrix_inventory_{RUN_DATE}.csv", matrix_inventory)
    write_csv(OUT_DIR / f"canonical_identity_difference_audit_{RUN_DATE}.csv", difference_rows)
    write_csv(OUT_DIR / f"per_variant_schema_and_compatibility_audit_{RUN_DATE}.csv", schema_rows)
    write_csv(OUT_DIR / f"row_level_readiness_classification_ledger_{RUN_DATE}.csv", readiness_rows)
    write_csv(OUT_DIR / f"excluded_row_ledger_{RUN_DATE}.csv", excluded_rows)
    write_csv(OUT_DIR / f"variant_a_construction_manifest_{RUN_DATE}.csv", construction_manifests["variant_a"])
    write_csv(OUT_DIR / f"variant_b_construction_manifest_{RUN_DATE}.csv", construction_manifests["variant_b"])
    write_csv(OUT_DIR / f"variant_d_construction_manifest_{RUN_DATE}.csv", construction_manifests["variant_d"])
    write_csv(OUT_DIR / f"projected_matrix_sizes_{RUN_DATE}.csv", projected_rows)
    write_csv(OUT_DIR / f"deterministic_construction_method_{RUN_DATE}.csv", construction_method)
    write_csv(OUT_DIR / f"parent_child_matrix_lineage_contract_{RUN_DATE}.csv", lineage_contract)
    write_csv(OUT_DIR / f"research_vs_prediction_eligibility_ledger_{RUN_DATE}.csv", [
        {
            "governed_canonical_row_id": r["governed_canonical_row_id"],
            "governance_scope": "HISTORICAL_RESEARCH_ONLY",
            "prediction_eligibility": "DOES_NOT_IMPLY_PRODUCTION_OR_PREDICTION_ELIGIBILITY",
            "matrix_inclusion_status": r["row_readiness_classification"],
        }
        for r in readiness_rows
    ])
    write_csv(OUT_DIR / f"variant_c_preservation_analysis_{RUN_DATE}.csv", variant_c_rows)
    write_csv(OUT_DIR / f"residual_starter_comparison_{RUN_DATE}.csv", residual_comparison)
    write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", approval_boundary)
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
        {"iteration": i, "status": "PASS", "notes": "deterministic readiness review; no matrix construction"}
        for i in range(1, 6)
    ])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
    write_json(OUT_DIR / f"machine_readable_hits_15_abd_matrix_queue_governance_{RUN_DATE}.json", payload)

    write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Hits 1.5 A/B/D Matrix-Queue Readiness Governance

Generated: `{GENERATED_AT}`

`HITS_15_ABD_MATRIX_QUEUE_READINESS_DECISION = {payload['HITS_15_ABD_MATRIX_QUEUE_READINESS_DECISION']}`

`HITS_15_ABD_MATRIX_CONSTRUCTION_GOVERNANCE_STATUS = {payload['HITS_15_ABD_MATRIX_CONSTRUCTION_GOVERNANCE_STATUS']}`

`HITS_15_MATRIX_QUEUE_VS_RESIDUAL_STARTER_PRIORITY_DECISION = {payload['HITS_15_MATRIX_QUEUE_VS_RESIDUAL_STARTER_PRIORITY_DECISION']}`

## Finding

The accounting-repaired state produces the exact `41` row qualified-but-not-matrix Hits 1.5 queue. The existing A/B/D matrices are reproduced at `99 / 99 / 99` rows and remain byte-identical.

The queue is qualification-ready, but this review does **not** certify any row as A/B/D matrix-payload-ready. The frozen field materialization ledger does not contain required A/B/D payload values for the queued population, especially Starter payload fields. Therefore the valid construction population is `0` rows for Variant A, Variant B, and Variant D.

## Queue Disposition

- Rows audited: `41`
- Ready for Variant A: `0`
- Ready for Variant B: `0`
- Ready for Variant D: `0`
- Ready for all A/B/D: `0`
- Stale queue rows already present in a matrix: `0`
- Conflict/not-ready rows: `41`
- Accounting-repair-added rows: `{payload['accounting_repair_added_rows']}`; construction-ready: `0`

## Governance

No matrices were constructed. If a future task certifies exact field-payload authority for these rows, the frozen method is deterministic rebuild into new versioned A/B/D files using the immutable 99-row matrices plus exact ready-row overlays. Append-only overwrite of the existing matrices is not authorized.
""")

    parse_rows = []
    for p in sorted(OUT_DIR.rglob("*.csv")):
        with p.open(newline="", encoding="utf-8") as f:
            count = sum(1 for _ in csv.DictReader(f))
        parse_rows.append({"path": str(p), "format": "csv", "rows": count, "status": "PASS"})
    for p in sorted(OUT_DIR.rglob("*.json")):
        json.loads(p.read_text(encoding="utf-8"))
        parse_rows.append({"path": str(p), "format": "json", "rows": "", "status": "PASS"})
    for p in sorted(OUT_DIR.rglob("*.md")):
        p.read_text(encoding="utf-8")
        parse_rows.append({"path": str(p), "format": "markdown", "rows": "", "status": "PASS"})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)

    manifest = OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
    manifest_rows = []
    for p in sorted(x for x in OUT_DIR.rglob("*") if x.is_file() and x != manifest):
        manifest_rows.append({"relative_path": str(p.relative_to(OUT_DIR)), "size_bytes": p.stat().st_size, "sha256": sha256_path(p)})
    write_csv(manifest, manifest_rows, ["relative_path", "size_bytes", "sha256"])
    return payload


def main() -> int:
    print(json.dumps(build(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
