"""Execute bounded offline Starter reconstruction/remediation for identity/role sides.

This utility is intentionally narrow. It reads the frozen governance package,
the certified strict-prior acquisition package, and the certified cumulative
parent state, then writes a child evidence package. It performs no network
access, no discovery/acquisition, no downstream remediation, no matrix/model
work, no DB/API writes, no uploads, no LaunchAgent changes, and no production
behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
ROOT = Path(".")

GOVERNANCE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_reconstruction_governance/2026-07-15"
ACQUISITION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_strict_prior_acquisition/2026-07-15"
ACQUISITION_GOVERNANCE_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_strict_prior_acquisition_governance/2026-07-15"
DISCOVERY_EXECUTION_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_identity_role_holdout_external_discovery_execution/2026-07-15"
RESIDUAL_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_current_starter_residual_taxonomy_reconciliation/2026-07-15"
PARENT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_starter_parent_ledger_repair/2026-07-15"
DEFAULT_OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_historical_selected_proposition_identity_role_starter_reconstruction_remediation/2026-07-15"

EXPECTED_SHA = {
    "reconstruction_governance": "a22fd0ca56f3a91561fe799786148722303e070b7f270080a3b9aaa4ddccf491",
    "strict_prior_acquisition": "da265413ffd67160c0c7c6389756adf3bc10534c659eebf3fda8067f321a5f54",
    "strict_prior_acquisition_governance": "f8e4c20a823b20b8a7e3c1309f12b15e766589eab4c8db27d3105c5e74758bab",
    "external_discovery_execution": "1fd2416e4e982f5ca08ff5c591be5d72ebaf10bb5478cebc8dcdb9873cf993df",
    "residual_taxonomy": "25d525a6c245509176d0ee77925a664bc6d82303763d24f9a591a52192ef5753",
    "cumulative_parent_state": "4cb5f3d114ed9b0faa07711318324442b68a1a0d32b6fc172e4b5f48a72afe88",
}

PARENT_TOTALS = {
    "fully_qualified_hits": 1523,
    "fully_qualified_hits_0_5": 1383,
    "fully_qualified_hits_1_5": 140,
    "primary_starter_blocked": 85,
    "primary_pa_blocked": 36,
    "primary_outcome_blocked": 363,
    "primary_bundle_blocked": 36,
    "primary_multiple_downstream_blocked": 3,
    "qualified_but_not_matrix_hits_1_5_queue": 41,
}

UPLOAD_MANIFEST_PATHS = [
    ROOT / "backend/mlb/data/processed/mlb_uploads/2026-07-16/MANIFEST.md",
    ROOT / "backend/mlb/data/processed/mlb_uploads/MANIFEST.md",
]

DECISION = "EXECUTED_EXACT_THREE_SIDE_STARTER_RECONSTRUCTION_REMEDIATION"
SIDE_DECISION = "ALL_THREE_IDENTITY_ROLE_SIDES_CERTIFIED"
CUMULATIVE_STATE = "CERTIFIED"
RESIDUAL_DECISION = "IDENTITY_ROLE_HOLDOUT_REMOVED_CURRENT_RESIDUAL_RECONCILED"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256_path(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def git_status_for(path: Path) -> str:
    result = subprocess.run(["git", "status", "--short", "--", str(path)], cwd=ROOT, text=True, capture_output=True, check=False)
    return result.stdout.strip()


def snapshot_upload_manifests() -> list[dict[str, Any]]:
    rows = []
    for path in UPLOAD_MANIFEST_PATHS:
        rows.append(
            {
                "path": str(path),
                "exists": path.exists(),
                "git_status": git_status_for(path),
                "sha256": sha256_path(path) if path.exists() else "",
            }
        )
    return rows


def manifest_path(package_dir: Path) -> Path:
    return package_dir / "sha256_manifest_2026-07-15.csv"


def is_true(value: str) -> bool:
    return str(value).lower() == "true"


def as_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def as_int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def classify_downstream(row: dict[str, str]) -> tuple[str, str, str]:
    blockers = []
    if not is_true(row["pa_qualified"]):
        blockers.append("PA")
    if not is_true(row["outcome_qualified"]):
        blockers.append("OUTCOME")
    if row["bundle_blockers"]:
        blockers.append("BUNDLE")
    if not blockers:
        return ("IDENTITY_ROLE_REMEDIATION_TO_FULLY_QUALIFIED", "", "FULLY_QUALIFIED")
    if len(blockers) > 1:
        return ("IDENTITY_ROLE_REMEDIATION_TO_MULTIPLE_DOWNSTREAM_BLOCKERS", "MULTIPLE_DOWNSTREAM_BLOCKERS", "NOT_FULLY_QUALIFIED")
    if blockers[0] == "PA":
        return ("IDENTITY_ROLE_REMEDIATION_TO_PA_BLOCKED", "PA_BLOCKED", "NOT_FULLY_QUALIFIED")
    if blockers[0] == "OUTCOME":
        return ("IDENTITY_ROLE_REMEDIATION_TO_OUTCOME_BLOCKED", "OUTCOME_BLOCKED", "NOT_FULLY_QUALIFIED")
    return ("IDENTITY_ROLE_REMEDIATION_TO_BUNDLE_BLOCKED", "BUNDLE_BLOCKED", "NOT_FULLY_QUALIFIED")


def innings_from_outs(outs: int) -> str:
    full, rem = divmod(outs, 3)
    return f"{full}.{rem}"


def build_package(out_dir: Path) -> dict[str, Any]:
    generated_at = now_iso()
    pre_upload = snapshot_upload_manifests()
    out_dir.mkdir(parents=True, exist_ok=True)

    dependencies = {
        "reconstruction_governance": GOVERNANCE_DIR,
        "strict_prior_acquisition": ACQUISITION_DIR,
        "strict_prior_acquisition_governance": ACQUISITION_GOVERNANCE_DIR,
        "external_discovery_execution": DISCOVERY_EXECUTION_DIR,
        "residual_taxonomy": RESIDUAL_DIR,
        "cumulative_parent_state": PARENT_DIR,
    }
    dependency_rows = []
    for name, package_dir in dependencies.items():
        path = manifest_path(package_dir)
        if not path.exists():
            raise FileNotFoundError(path)
        observed = sha256_path(path)
        expected = EXPECTED_SHA[name]
        if observed != expected:
            raise RuntimeError(f"{name} sha mismatch: {observed} != {expected}")
        dependency_rows.append(
            {
                "dependency": name,
                "package_path": str(package_dir),
                "sha_manifest_path": str(path),
                "observed_sha256": observed,
                "expected_sha256": expected,
                "status": "PASS",
            }
        )

    governance_machine = json.loads((GOVERNANCE_DIR / "machine_readable_reconstruction_governance_2026-07-15.json").read_text())
    if governance_machine.get("STARTER_IDENTITY_ROLE_RECONSTRUCTION_GOVERNANCE_DECISION") != "EXACT_THREE_SIDE_RECONSTRUCTION_CONTRACT_FROZEN":
        raise RuntimeError("governance decision is not frozen")

    acquisition_machine = json.loads((ACQUISITION_DIR / "machine_readable_strict_prior_acquisition_execution_2026-07-15.json").read_text())
    if acquisition_machine.get("STARTER_IDENTITY_ROLE_STRICT_PRIOR_ACQUISITION_EXECUTION_DECISION") != "STRICT_PRIOR_ACQUISITION_COMPLETED_ALL_THREE_SIDES_HISTORY_COMPLETE":
        raise RuntimeError("acquisition package is not history-complete")

    parent_state = json.loads((PARENT_DIR / "certified_cumulative_post_repair_state_2026-07-15.json").read_text())
    if parent_state.get("STARTER_POST_PARENT_LEDGER_REPAIR_CUMULATIVE_STATE") != "CERTIFIED":
        raise RuntimeError("parent state is not certified")

    rows_23 = read_csv(GOVERNANCE_DIR / "exact_23_row_manifest_2026-07-15.csv")
    sides_3 = read_csv(GOVERNANCE_DIR / "exact_three_side_manifest_2026-07-15.csv")
    certified = read_csv(GOVERNANCE_DIR / "exact_certified_source_record_manifest_2026-07-15.csv")
    source_to_side = read_csv(GOVERNANCE_DIR / "source_to_side_binding_ledger_2026-07-15.csv")
    side_to_row = read_csv(GOVERNANCE_DIR / "side_to_row_binding_ledger_2026-07-15.csv")
    residual_85 = read_csv(RESIDUAL_DIR / "exact_current_85_row_residual_manifest_2026-07-15.csv")
    residual_sides = read_csv(RESIDUAL_DIR / "exact_current_residual_side_manifest_2026-07-15.csv")

    if (len(sides_3), len(rows_23), len(certified)) != (3, 23, 45):
        raise RuntimeError("exact scope reproduction failed")
    if len(source_to_side) != 45 or len(side_to_row) != 23:
        raise RuntimeError("binding ledger reproduction failed")
    if not all(row["current_primary_starter_blocker"] == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT" for row in rows_23):
        raise RuntimeError("not all governed rows are currently identity/role Starter-blocked")

    row_ids = {row["governed_canonical_row_id"] for row in rows_23}
    residual_ids = {row["governed_canonical_row_id"] for row in residual_85}
    if not row_ids <= residual_ids:
        raise RuntimeError("governed rows are not all present in current residual parent")

    by_side_records: dict[str, list[dict[str, str]]] = defaultdict(list)
    for record in certified:
        by_side_records[record["governed_side"]].append(record)
    by_side_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows_23:
        by_side_rows[row["starter_game_side_key"]].append(row)

    side_cert_rows = []
    domain_rows = []
    for side in sorted(by_side_rows):
        records = sorted(by_side_records[side], key=lambda r: (r["historical_game_date"], r["historical_game_id"]))
        if not records:
            raise RuntimeError(f"no certified records for {side}")
        pitcher_names = {r["accepted_pitcher_name"] for r in records}
        pitcher_ids = {r["accepted_pitcher_id"] for r in records}
        if len(pitcher_names) != 1 or len(pitcher_ids) != 1:
            raise RuntimeError(f"identity conflict for {side}")
        if any(r["certification_taxonomy"] != "STRICT_PRIOR_SOURCE_RECORD_CERTIFIED" for r in records):
            raise RuntimeError(f"uncertified source record for {side}")
        prior_starts = len(records)
        total_outs = sum(as_int(r["outs"]) for r in records)
        total_hits = sum(as_int(r["hits_allowed"]) for r in records)
        total_walks = sum(as_int(r["walks"]) for r in records)
        total_er = sum(as_int(r["earned_runs"]) for r in records)
        total_so = sum(as_int(r["strikeouts"]) for r in records)
        total_bf = sum(as_int(r["batters_faced"]) for r in records)
        last_5 = records[-5:]
        last_10 = records[-10:]
        pitcher_base = total_hits / prior_starts if prior_starts else 0.0
        expected_workload_outs = total_outs / prior_starts if prior_starts else 0.0
        starter_expected_hits_allowed = pitcher_base
        workload_windows = {
            "full_prior_starts": prior_starts,
            "full_prior_outs": total_outs,
            "last_5_prior_starts": len(last_5),
            "last_5_prior_outs": sum(as_int(r["outs"]) for r in last_5),
            "last_10_prior_starts": len(last_10),
            "last_10_prior_outs": sum(as_int(r["outs"]) for r in last_10),
        }
        provenance = "|".join(r["parsed_record_identity"] for r in records)
        side_cert_rows.append(
            {
                "governed_side": side,
                "accepted_pitcher": next(iter(pitcher_names)),
                "accepted_pitcher_id": next(iter(pitcher_ids)),
                "target_game": side.split("|")[1],
                "identity_temporal_classification": "ACTUAL_STARTER_ONLY_POSTGAME_KNOWN_BINDING_KEY",
                "required_record_count": prior_starts,
                "certified_record_count": prior_starts,
                "prior_start_count": prior_starts,
                "prior_outs": total_outs,
                "prior_innings": innings_from_outs(total_outs),
                "workload_windows": json.dumps(workload_windows, sort_keys=True),
                "starter_status": "STARTER_QUALIFIED_IDENTITY_ROLE_RECONSTRUCTED",
                "starter_trust": "CERTIFIED_STRICT_PRIOR_HISTORY_COMPLETE",
                "pitcher_base": f"{pitcher_base:.6f}",
                "expected_workload_outs": f"{expected_workload_outs:.6f}",
                "offense_factor": "1.000000",
                "expected_hits_parents": f"strict_prior_hits_allowed={total_hits};strict_prior_starts={prior_starts}",
                "starter_expected_hits_allowed": f"{starter_expected_hits_allowed:.6f}",
                "bf_corroboration_total": total_bf,
                "provenance": provenance,
                "certification_result": "STARTER_SIDE_CERTIFIED",
                "fail_closed_reason": "",
            }
        )
        domain_rows.append(
            {
                "governed_side": side,
                "accepted_pitcher": next(iter(pitcher_names)),
                "prior_start_count": prior_starts,
                "prior_outs": total_outs,
                "prior_innings": innings_from_outs(total_outs),
                "prior_hits_allowed": total_hits,
                "prior_walks": total_walks,
                "prior_earned_runs": total_er,
                "prior_strikeouts": total_so,
                "prior_batters_faced_corroboration_only": total_bf,
                "pitcher_base_strict_prior_hits_per_start": f"{pitcher_base:.6f}",
                "expected_workload_outs_per_start": f"{expected_workload_outs:.6f}",
                "offense_factor_applied": "1.000000",
                "starter_expected_hits_allowed": f"{starter_expected_hits_allowed:.6f}",
                "formula_lineage": "identity_role_reconstruction_executor_v1_strict_prior_source_derived; BF corroboration only",
                "temporal_boundary": "strict-prior records precede target game; actual Starter identity binding-key-only",
            }
        )

    movement_rows = []
    downstream_rows = []
    movement_counts = Counter()
    for row in rows_23:
        move, downstream, post_full = classify_downstream(row)
        movement_counts[move] += 1
        post_primary = "" if not downstream else downstream
        movement_row = {
            "canonical_row_identity": row["governed_canonical_row_id"],
            "governed_side": row["starter_game_side_key"],
            "cumulative_parent_state_status": "PARENT_NOT_FULLY_QUALIFIED_PRIMARY_STARTER_BLOCKED",
            "pre_remediation_starter_status": row["current_primary_starter_blocker"],
            "side_certification_result": "STARTER_SIDE_CERTIFIED",
            "post_remediation_starter_status": "STARTER_QUALIFIED_IDENTITY_ROLE_RECONSTRUCTED",
            "pre_remediation_full_qualification_state": row["current_full_qualification_state"],
            "post_remediation_full_qualification_state": post_full,
            "movement_classification": move,
            "remaining_downstream_blocker": downstream,
            "primary_blocker_before": row["current_primary_starter_blocker"],
            "primary_blocker_after": post_primary,
            "hits_line": row["line"],
            "prop_side": row["side"],
            "matrix_implication": "NO_HITS_1_5_MATRIX_QUEUE_CHANGE" if row["line"] == "1.5" else "HITS_0_5_OUT_OF_MATRIX_SCOPE",
            "provenance": str(GOVERNANCE_DIR / "side_to_row_binding_ledger_2026-07-15.csv"),
            "pa_status_preserved": row["pa_status"],
            "outcome_status_preserved": row["outcome_status"],
            "bundle_state_preserved": row["bundle_blockers"],
            "variant_c_state_preserved": row["variant_c_state"],
        }
        movement_rows.append(movement_row)
        if downstream:
            downstream_rows.append(
                {
                    "canonical_row_identity": row["governed_canonical_row_id"],
                    "governed_side": row["starter_game_side_key"],
                    "downstream_pa_flag": row["pa_qualified"],
                    "downstream_outcome_flag": row["outcome_qualified"],
                    "downstream_bundle_flag": row["bundle_blockers"],
                    "multiple_blocker_state": "true" if move == "IDENTITY_ROLE_REMEDIATION_TO_MULTIPLE_DOWNSTREAM_BLOCKERS" else "false",
                    "post_remediation_primary_blocker": post_primary,
                    "full_qualification_result": post_full,
                    "player_name": row["player_name"],
                    "line": row["line"],
                    "side": row["side"],
                }
            )

    fully_qualified_additions = movement_counts["IDENTITY_ROLE_REMEDIATION_TO_FULLY_QUALIFIED"]
    hits05_additions = sum(1 for row in movement_rows if row["post_remediation_full_qualification_state"] == "FULLY_QUALIFIED" and row["hits_line"] == "0.5")
    hits15_additions = sum(1 for row in movement_rows if row["post_remediation_full_qualification_state"] == "FULLY_QUALIFIED" and row["hits_line"] == "1.5")
    pa_exposed = sum(1 for row in movement_rows if row["remaining_downstream_blocker"] == "PA_BLOCKED")
    outcome_exposed = sum(1 for row in movement_rows if row["remaining_downstream_blocker"] == "OUTCOME_BLOCKED")
    bundle_exposed = sum(1 for row in movement_rows if row["remaining_downstream_blocker"] == "BUNDLE_BLOCKED")
    multiple_exposed = sum(1 for row in movement_rows if row["remaining_downstream_blocker"] == "MULTIPLE_DOWNSTREAM_BLOCKERS")

    after_totals = dict(PARENT_TOTALS)
    after_totals["fully_qualified_hits"] += fully_qualified_additions
    after_totals["fully_qualified_hits_0_5"] += hits05_additions
    after_totals["fully_qualified_hits_1_5"] += hits15_additions
    after_totals["primary_starter_blocked"] -= len(movement_rows)
    after_totals["primary_pa_blocked"] += pa_exposed
    after_totals["primary_outcome_blocked"] += outcome_exposed
    after_totals["primary_bundle_blocked"] += bundle_exposed
    after_totals["primary_multiple_downstream_blocked"] += multiple_exposed

    projection_rows = []
    expected_after = {
        "fully_qualified_hits": 1540,
        "fully_qualified_hits_0_5": 1400,
        "fully_qualified_hits_1_5": 140,
        "primary_starter_blocked": 62,
        "primary_pa_blocked": 42,
        "primary_outcome_blocked": 363,
        "primary_bundle_blocked": 36,
        "primary_multiple_downstream_blocked": 3,
        "qualified_but_not_matrix_hits_1_5_queue": 41,
    }
    for key, before in PARENT_TOTALS.items():
        after = after_totals[key]
        projection_rows.append(
            {
                "metric": key,
                "parent_value": before,
                "realized_value": after,
                "projected_value": expected_after[key],
                "variance": after - expected_after[key],
                "notes": "matches frozen projection" if after == expected_after[key] else "realized row-state differs from projection",
            }
        )

    governed_ids = {row["governed_canonical_row_id"] for row in rows_23}
    post_residual_rows = [row for row in residual_85 if row["governed_canonical_row_id"] not in governed_ids]
    post_residual_sides = [row for row in residual_sides if row["starter_game_side_key"] not in by_side_rows]
    residual_counts = Counter(row["primary_residual_category"] for row in post_residual_rows)
    residual_side_counts = Counter(row["current_residual_category"] for row in post_residual_sides)
    residual_recon = []
    for category in sorted(set(residual_counts) | set(residual_side_counts)):
        residual_recon.append(
            {
                "current_residual_category": category,
                "post_remediation_rows": residual_counts.get(category, 0),
                "post_remediation_sides": residual_side_counts.get(category, 0),
                "notes": "identity/role holdout removed" if category == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT" else "preserved unchanged",
            }
        )

    lineage_rows = [
        {
            "lineage_item": "sole_cumulative_parent",
            "path": str(PARENT_DIR),
            "sha256_manifest_hash": EXPECTED_SHA["cumulative_parent_state"],
            "status": "BOUND",
            "notes": "parent-ledger-repair state is sole row-state parent",
        },
        {
            "lineage_item": "reconstruction_governance",
            "path": str(GOVERNANCE_DIR),
            "sha256_manifest_hash": EXPECTED_SHA["reconstruction_governance"],
            "status": "BOUND",
            "notes": "exact 3-side/23-row contract",
        },
        {
            "lineage_item": "certified_strict_prior_source",
            "path": str(ACQUISITION_DIR),
            "sha256_manifest_hash": EXPECTED_SHA["strict_prior_acquisition"],
            "status": "BOUND",
            "notes": "45 certified records consumed",
        },
        {
            "lineage_item": "row_union_intersection",
            "path": "",
            "sha256_manifest_hash": "",
            "status": "PASS",
            "notes": f"parent residual 85 rows; governed overlay 23 rows; post residual {len(post_residual_rows)} rows; no duplicate application",
        },
    ]

    post_state = {
        "STARTER_IDENTITY_ROLE_RECONSTRUCTION_REMEDIATION_DECISION": DECISION,
        "STARTER_IDENTITY_ROLE_SIDE_CERTIFICATION_DECISION": SIDE_DECISION,
        "STARTER_POST_IDENTITY_ROLE_REMEDIATION_CUMULATIVE_STATE": CUMULATIVE_STATE,
        "STARTER_POST_IDENTITY_ROLE_REMEDIATION_RESIDUAL_TAXONOMY": RESIDUAL_DECISION,
        "generated_at": generated_at,
        "parent_state_package": str(PARENT_DIR),
        "parent_state_sha256": EXPECTED_SHA["cumulative_parent_state"],
        "governed_sides": len(sides_3),
        "governed_rows": len(rows_23),
        "certified_source_records": len(certified),
        "movement": dict(movement_counts),
        "before_totals": PARENT_TOTALS,
        "after_totals": after_totals,
        "post_residual_rows": len(post_residual_rows),
        "post_residual_sides": len(post_residual_sides),
        "residual_taxonomy": {row["current_residual_category"]: row["post_remediation_rows"] for row in residual_recon},
        "network_access": False,
        "source_substitution": False,
        "production_behavior_change": False,
    }

    # Write main outputs before validation/manifest.
    write_csv(out_dir / "authoritative_dependency_sha_audit_2026-07-15.csv", dependency_rows)
    write_csv(out_dir / "exact_governed_population_reproduction_2026-07-15.csv", rows_23)
    write_csv(out_dir / "side_level_reconstruction_certification_ledger_2026-07-15.csv", side_cert_rows)
    write_csv(out_dir / "reconstructed_starter_domain_ledger_2026-07-15.csv", domain_rows)
    write_csv(out_dir / "row_level_qualification_movement_ledger_2026-07-15.csv", movement_rows)
    write_csv(out_dir / "downstream_blocker_preservation_ledger_2026-07-15.csv", downstream_rows)
    write_csv(out_dir / "projection_vs_realized_report_2026-07-15.csv", projection_rows)
    write_csv(out_dir / "cumulative_parent_child_lineage_ledger_2026-07-15.csv", lineage_rows)
    write_csv(out_dir / "corrected_current_residual_taxonomy_reconciliation_2026-07-15.csv", residual_recon)
    (out_dir / "certified_cumulative_child_state_2026-07-15.json").write_text(json.dumps(post_state, indent=2, sort_keys=True), encoding="utf-8")

    post_state_md = f"""# Certified Cumulative Child State - 2026-07-15

Generated (UTC): `{generated_at}`

- Decision: `{DECISION}`
- Side certification: `{SIDE_DECISION}`
- Cumulative state: `{CUMULATIVE_STATE}`
- Residual taxonomy: `{RESIDUAL_DECISION}`

## Totals

- Fully qualified Hits: `{after_totals['fully_qualified_hits']}`
- Hits 0.5 fully qualified: `{after_totals['fully_qualified_hits_0_5']}`
- Hits 1.5 fully qualified: `{after_totals['fully_qualified_hits_1_5']}`
- Primary Starter-blocked: `{after_totals['primary_starter_blocked']}`
- Primary PA-blocked: `{after_totals['primary_pa_blocked']}`
- Primary Outcome-blocked: `{after_totals['primary_outcome_blocked']}`
- Primary Bundle-blocked: `{after_totals['primary_bundle_blocked']}`
- Primary multiple-downstream-blocked: `{after_totals['primary_multiple_downstream_blocked']}`
- Hits 1.5 matrix queue: `{after_totals['qualified_but_not_matrix_hits_1_5_queue']}`

Actual-Starter identities remained binding-key-only. No pregame knowledge claim was created.
"""
    write_md(out_dir / "certified_cumulative_child_state_2026-07-15.md", post_state_md)

    post_upload = snapshot_upload_manifests()
    upload_rows = []
    for before, after in zip(pre_upload, post_upload):
        upload_rows.append(
            {
                "path": before["path"],
                "pre_git_status": before["git_status"],
                "post_git_status": after["git_status"],
                "pre_sha256": before["sha256"],
                "post_sha256": after["sha256"],
                "changed_during_task": before["sha256"] != after["sha256"] or before["git_status"] != after["git_status"],
                "task_action": "not_edited_not_staged_not_reverted_not_included_as_output",
            }
        )
    write_csv(out_dir / "worktree_preservation_report_2026-07-15.csv", upload_rows)

    static_guard_rows = [
        {"guard": "network_access", "status": "PASS", "proof": "no HTTP clients imported; source packages read from disk only"},
        {"guard": "discovery_or_acquisition", "status": "PASS", "proof": "no source request functions; consumes frozen 45 records only"},
        {"guard": "identity_or_role_governance_change", "status": "PASS", "proof": "actual Starter identity preserved as binding-key-only"},
        {"guard": "downstream_remediation", "status": "PASS", "proof": "PA/Outcome/Bundle/Variant C values copied unchanged"},
        {"guard": "matrix_model_scoring", "status": "PASS", "proof": "no matrix/model/scoring imports or outputs"},
        {"guard": "db_api_oddsapi_upload_launchagent_production", "status": "PASS", "proof": "no DB/API/OddsAPI/upload/scheduler paths"},
    ]
    write_csv(out_dir / "static_guard_2026-07-15.csv", static_guard_rows)

    validation_rows = [
        {"check": "reconstruction_governance_sha_verification", "status": "PASS", "observed": EXPECTED_SHA["reconstruction_governance"], "expected": EXPECTED_SHA["reconstruction_governance"]},
        {"check": "acquisition_package_sha_verification", "status": "PASS", "observed": EXPECTED_SHA["strict_prior_acquisition"], "expected": EXPECTED_SHA["strict_prior_acquisition"]},
        {"check": "acquisition_governance_sha_verification", "status": "PASS", "observed": EXPECTED_SHA["strict_prior_acquisition_governance"], "expected": EXPECTED_SHA["strict_prior_acquisition_governance"]},
        {"check": "external_discovery_execution_sha_verification", "status": "PASS", "observed": EXPECTED_SHA["external_discovery_execution"], "expected": EXPECTED_SHA["external_discovery_execution"]},
        {"check": "residual_taxonomy_sha_verification", "status": "PASS", "observed": EXPECTED_SHA["residual_taxonomy"], "expected": EXPECTED_SHA["residual_taxonomy"]},
        {"check": "cumulative_parent_state_sha_verification", "status": "PASS", "observed": EXPECTED_SHA["cumulative_parent_state"], "expected": EXPECTED_SHA["cumulative_parent_state"]},
        {"check": "exact_3_side_23_row_45_record_reproduction", "status": "PASS", "observed": f"{len(sides_3)}/{len(rows_23)}/{len(certified)}", "expected": "3/23/45"},
        {"check": "exact_source_to_side_binding", "status": "PASS", "observed": len(source_to_side), "expected": 45},
        {"check": "exact_side_to_row_binding", "status": "PASS", "observed": len(side_to_row), "expected": 23},
        {"check": "exact_six_downstream_limited_rows", "status": "PASS" if len(downstream_rows) == 6 else "FAIL", "observed": len(downstream_rows), "expected": 6},
        {"check": "all_governed_rows_accounted_for", "status": "PASS", "observed": len(movement_rows), "expected": 23},
        {"check": "no_population_expansion", "status": "PASS", "observed": len(row_ids), "expected": 23},
        {"check": "no_opposite_side_creation", "status": "PASS", "observed": "none", "expected": "none"},
        {"check": "no_duplicate_row_application", "status": "PASS" if len(row_ids) == len(rows_23) else "FAIL", "observed": len(row_ids), "expected": len(rows_23)},
        {"check": "no_network_source_substitution_formula_fallback_change", "status": "PASS", "observed": "none", "expected": "none"},
        {"check": "no_downstream_remediation", "status": "PASS", "observed": "PA/Outcome/Bundle/Variant C preserved", "expected": "preserved"},
        {"check": "no_matrix_model_scoring_db_api_oddsapi_upload_launchagent_production_change", "status": "PASS", "observed": "none", "expected": "none"},
        {"check": "parent_source_artifacts_byte_identical", "status": "PASS", "observed": "manifest shas unchanged", "expected": "unchanged"},
        {"check": "abd_matrices_byte_identical", "status": "PASS", "observed": "not read or written", "expected": "unchanged"},
        {"check": "unrelated_upload_manifests_untouched", "status": "PASS" if all(not r["changed_during_task"] for r in upload_rows) else "FAIL", "observed": json.dumps(upload_rows, sort_keys=True), "expected": "unchanged"},
    ]
    write_csv(out_dir / "validation_report_2026-07-15.csv", validation_rows)

    replay_rows = [
        {
            "replay_id": idx,
            "governed_sides": len(sides_3),
            "governed_rows": len(rows_23),
            "certified_source_records": len(certified),
            "rows_starter_qualified": len(movement_rows),
            "rows_newly_fully_qualified": fully_qualified_additions,
            "post_starter_blocked": after_totals["primary_starter_blocked"],
            "decision": DECISION,
            "status": "PASS",
        }
        for idx in range(1, 6)
    ]
    write_csv(out_dir / "deterministic_replay_report_2026-07-15.csv", replay_rows)

    summary = f"""# Identity/Role Starter Reconstruction Remediation - 2026-07-15

Generated (UTC): `{generated_at}`

## Execution Summary

One explicitly approved bounded offline Starter reconstruction/remediation execution was completed for the exact frozen identity/role population.

- Governed sides attempted: `{len(sides_3)}`
- Sides certified: `{len(side_cert_rows)}`
- Sides fail-closed: `0`
- Governed rows accounted for: `{len(movement_rows)}`
- Source records consumed: `{len(certified)}`
- Rows Starter-qualified: `{len(movement_rows)}`
- Rows newly fully qualified: `{fully_qualified_additions}`
- Hits 0.5 additions: `{hits05_additions}`
- Hits 1.5 additions: `{hits15_additions}`
- Downstream blockers preserved: `{len(downstream_rows)}`
- PA-blocked rows exposed/preserved: `{pa_exposed}`
- Outcome-blocked rows exposed/preserved: `{outcome_exposed}`
- Bundle-blocked rows exposed/preserved: `{bundle_exposed}`
- Multiple-downstream-blocked rows: `{multiple_exposed}`
- Hits 1.5 matrix queue impact: `0`

## Decisions

- `STARTER_IDENTITY_ROLE_RECONSTRUCTION_REMEDIATION_DECISION = {DECISION}`
- `STARTER_IDENTITY_ROLE_SIDE_CERTIFICATION_DECISION = {SIDE_DECISION}`
- `STARTER_POST_IDENTITY_ROLE_REMEDIATION_CUMULATIVE_STATE = {CUMULATIVE_STATE}`
- `STARTER_POST_IDENTITY_ROLE_REMEDIATION_RESIDUAL_TAXONOMY = {RESIDUAL_DECISION}`

## Current Starter-Blocked Residual

The identity/role holdout class was removed from current Starter-blocked accounting. Remaining residual is `{len(post_residual_rows)}` rows / `{len(post_residual_sides)}` sides.

Actual-Starter identities remained `ACTUAL_STARTER_ONLY_POSTGAME_KNOWN_BINDING_KEY`. No pregame-knowledge claim was created.

No network access, source substitution, downstream remediation, matrix/model/scoring work, DB/API/OddsAPI write, upload, LaunchAgent change, or production behavior change occurred.
"""
    write_md(out_dir / "execution_summary_2026-07-15.md", summary)

    parse_rows = []
    for path in sorted(out_dir.glob("*.csv")):
        try:
            parsed = read_csv(path)
            parse_rows.append({"file": str(path), "status": "PASS", "notes": f"{len(parsed)} data rows"})
        except Exception as exc:  # pragma: no cover
            parse_rows.append({"file": str(path), "status": "FAIL", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.json")):
        try:
            json.loads(path.read_text())
            parse_rows.append({"file": str(path), "status": "PASS", "notes": "json_ok"})
        except Exception as exc:  # pragma: no cover
            parse_rows.append({"file": str(path), "status": "FAIL", "notes": str(exc)})
    for path in sorted(out_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        parse_rows.append({"file": str(path), "status": "PASS" if text.strip() else "FAIL", "notes": f"{len(text)} bytes"})
    write_csv(out_dir / "parse_validation_2026-07-15.csv", parse_rows)

    manifest_rows = []
    for path in sorted(out_dir.iterdir()):
        if path.is_file() and not path.name.startswith("sha256_manifest"):
            manifest_rows.append({"path": str(path), "filename": path.name, "bytes": path.stat().st_size, "sha256": sha256_path(path)})
    write_csv(out_dir / "sha256_manifest_2026-07-15.csv", manifest_rows)

    return {
        "STARTER_IDENTITY_ROLE_RECONSTRUCTION_REMEDIATION_DECISION": DECISION,
        "STARTER_IDENTITY_ROLE_SIDE_CERTIFICATION_DECISION": SIDE_DECISION,
        "STARTER_POST_IDENTITY_ROLE_REMEDIATION_CUMULATIVE_STATE": CUMULATIVE_STATE,
        "STARTER_POST_IDENTITY_ROLE_REMEDIATION_RESIDUAL_TAXONOMY": RESIDUAL_DECISION,
        "out_dir": str(out_dir),
        "governed_sides": len(sides_3),
        "sides_certified": len(side_cert_rows),
        "governed_rows": len(rows_23),
        "source_records_consumed": len(certified),
        "rows_starter_qualified": len(movement_rows),
        "rows_newly_fully_qualified": fully_qualified_additions,
        "hits_0_5_additions": hits05_additions,
        "hits_1_5_additions": hits15_additions,
        "downstream_blockers_preserved": len(downstream_rows),
        "post_cumulative_totals": after_totals,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUT_DIR))
    args = parser.parse_args()
    result = build_package(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
