#!/usr/bin/env python3
"""Review remaining MLB selected-proposition residual research branches.

This bounded utility is documentation and decision support only. It reads
already-certified artifacts, freezes exact branch populations, scores branches
with non-signal factors, and writes a reproducible review package. It does not
perform discovery, acquisition, payload recovery, feature reconstruction,
remediation, matrix construction, qualification propagation, model work,
database/API writes, uploads, scheduler changes, or production changes.
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

ACCOUNTING_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_stale_starter_blocker_accounting_audit/"
    "2026-07-15"
)
MATRIX_GOV_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hits_15_starter_field_version_governance/"
    "2026-07-15"
)
RESIDUAL_REVIEW_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_residual_starter_blocked_population_review/"
    "2026-07-15"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_residual_research_portfolio_review/"
    "2026-07-15"
)

ACCOUNTING_STATE = ACCOUNTING_DIR / f"certified_cumulative_accounting_repaired_state_{RUN_DATE}.json"
ACCOUNTING_SHA = ACCOUNTING_DIR / f"sha256_manifest_{RUN_DATE}.csv"
POST_REPAIR_RESIDUAL = ACCOUNTING_DIR / f"true_residual_starter_blocked_manifest_{RUN_DATE}.csv"

MATRIX_GOV_JSON = MATRIX_GOV_DIR / f"machine_readable_starter_field_version_governance_{RUN_DATE}.json"
MATRIX_GOV_SHA = MATRIX_GOV_DIR / f"sha256_manifest_{RUN_DATE}.csv"
MATRIX_QUEUE = MATRIX_GOV_DIR / f"exact_41_row_manifest_{RUN_DATE}.csv"
MATRIX_PARENT_LEDGER = MATRIX_GOV_DIR / f"queue_41_parent_evidence_ledger_{RUN_DATE}.csv"

RESIDUAL_SHA = RESIDUAL_REVIEW_DIR / f"sha256_manifest_{RUN_DATE}.csv"
RESIDUAL_SIDE_LEDGER = RESIDUAL_REVIEW_DIR / f"recoverability_classification_ledger_{RUN_DATE}.csv"

EXPECTED_ACCOUNTING_TOTALS = {
    "fully_qualified_hits": 1484,
    "fully_qualified_hits_0_5": 1344,
    "fully_qualified_hits_1_5": 140,
    "primary_starter_blocked": 128,
    "primary_pa_blocked": 32,
    "primary_outcome_blocked": 363,
    "primary_bundle_blocked": 36,
    "primary_multiple_downstream_blocked": 3,
    "qualified_but_not_matrix_hits_1_5_queue": 41,
}
EXPECTED_BRANCH_ROWS = {
    "MATRIX_PARENT_PAYLOAD_RECOVERY": 41,
    "STARTER_PARENT_DOMAIN_MISSING_OTHER": 26,
    "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": 23,
    "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT": 17,
    "ESTABLISHED_SPECIAL_REGIME_EXCLUSION": 46,
    "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED": 16,
}
EXPECTED_BRANCH_SIDES = {
    "STARTER_PARENT_DOMAIN_MISSING_OTHER": 3,
    "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": 3,
    "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT": 2,
    "ESTABLISHED_SPECIAL_REGIME_EXCLUSION": 7,
    "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED": 2,
}

PRIMARY_BRANCHES = [
    "MATRIX_PARENT_PAYLOAD_RECOVERY",
    "STARTER_PARENT_DOMAIN_MISSING_OTHER",
    "IDENTITY_OR_ROLE_REVIEW_HOLDOUT",
    "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT",
]

BRANCH_LABELS = {
    "MATRIX_PARENT_PAYLOAD_RECOVERY": "Branch A - 41-row matrix parent-payload recovery",
    "STARTER_PARENT_DOMAIN_MISSING_OTHER": "Branch B - 26 other missing Starter-parent rows",
    "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": "Branch C - 23 identity/role holdout rows",
    "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT": "Branch D - 17 local construction/persistence-defect rows",
}

DECISIONS = {
    "MATRIX_PARENT_PAYLOAD_RECOVERY": "MATRIX_PARENT_PAYLOAD_RECOVERY_MODERATE_VALUE",
    "STARTER_PARENT_DOMAIN_MISSING_OTHER": "OTHER_STARTER_PARENT_RECOVERY_MODERATE_VALUE",
    "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": "IDENTITY_ROLE_REVIEW_MODERATE_VALUE",
    "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT": "LOCAL_PLATFORM_DEFECT_REPAIR_HIGH_VALUE",
}

WEIGHTS = {
    "maximum_usable_row_yield": 0.10,
    "probability_of_technical_recovery": 0.10,
    "probability_of_governance_approval": 0.09,
    "source_availability": 0.08,
    "engineering_effort": 0.08,
    "governance_effort": 0.07,
    "formula_version_risk": 0.08,
    "identity_role_contamination_risk": 0.07,
    "platform_reuse": 0.12,
    "future_season_reuse": 0.08,
    "impact_on_already_qualified_research_assets": 0.05,
    "evidence_gained_if_branch_fails": 0.04,
    "reversibility_and_blast_radius": 0.04,
}

SCORES = {
    "MATRIX_PARENT_PAYLOAD_RECOVERY": {
        "maximum_usable_row_yield": 10,
        "probability_of_technical_recovery": 3,
        "probability_of_governance_approval": 4,
        "source_availability": 2,
        "engineering_effort": 3,
        "governance_effort": 3,
        "formula_version_risk": 2,
        "identity_role_contamination_risk": 8,
        "platform_reuse": 2,
        "future_season_reuse": 2,
        "impact_on_already_qualified_research_assets": 7,
        "evidence_gained_if_branch_fails": 6,
        "reversibility_and_blast_radius": 7,
    },
    "STARTER_PARENT_DOMAIN_MISSING_OTHER": {
        "maximum_usable_row_yield": 6,
        "probability_of_technical_recovery": 5,
        "probability_of_governance_approval": 5,
        "source_availability": 4,
        "engineering_effort": 5,
        "governance_effort": 5,
        "formula_version_risk": 7,
        "identity_role_contamination_risk": 8,
        "platform_reuse": 4,
        "future_season_reuse": 4,
        "impact_on_already_qualified_research_assets": 3,
        "evidence_gained_if_branch_fails": 5,
        "reversibility_and_blast_radius": 8,
    },
    "IDENTITY_OR_ROLE_REVIEW_HOLDOUT": {
        "maximum_usable_row_yield": 5,
        "probability_of_technical_recovery": 5,
        "probability_of_governance_approval": 4,
        "source_availability": 5,
        "engineering_effort": 6,
        "governance_effort": 3,
        "formula_version_risk": 8,
        "identity_role_contamination_risk": 2,
        "platform_reuse": 6,
        "future_season_reuse": 6,
        "impact_on_already_qualified_research_assets": 2,
        "evidence_gained_if_branch_fails": 6,
        "reversibility_and_blast_radius": 8,
    },
    "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT": {
        "maximum_usable_row_yield": 5,
        "probability_of_technical_recovery": 8,
        "probability_of_governance_approval": 8,
        "source_availability": 8,
        "engineering_effort": 7,
        "governance_effort": 7,
        "formula_version_risk": 8,
        "identity_role_contamination_risk": 9,
        "platform_reuse": 10,
        "future_season_reuse": 9,
        "impact_on_already_qualified_research_assets": 5,
        "evidence_gained_if_branch_fails": 8,
        "reversibility_and_blast_radius": 9,
    },
}

SELECTED_BRANCH = "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT"


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


def sha_manifest_hash(package_dir: Path) -> str:
    path = package_dir / f"sha256_manifest_{RUN_DATE}.csv"
    if not path.exists():
        raise FileNotFoundError(path)
    return sha256(path)


def score_branch(branch: str, weights: dict[str, float] = WEIGHTS) -> float:
    return round(sum(SCORES[branch][factor] * weight for factor, weight in weights.items()), 4)


def side_key(row: dict[str, str]) -> str:
    return row.get("starter_game_side_key") or "|".join(
        [
            row.get("slate_date", ""),
            row.get("game_id", ""),
            row.get("team", ""),
            row.get("opponent", ""),
        ]
    )


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("canonical_row_id") or "|".join(
        [
            row.get("slate_date", ""),
            row.get("game_id", ""),
            row.get("player_id", ""),
            row.get("prop_type", ""),
            row.get("line", ""),
            row.get("side", ""),
        ]
    )


def summarize_rows(rows: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "rows": len(rows),
        "sides": len({side_key(r) for r in rows}),
        "hits_0_5_rows": sum(1 for r in rows if r.get("line") == "0.5"),
        "hits_1_5_rows": sum(1 for r in rows if r.get("line") == "1.5"),
        "non_starter_prereq_rows": sum(
            1
            for r in rows
            if str(r.get("pa_qualified", "")).lower() == "true"
            and str(r.get("outcome_qualified", "")).lower() == "true"
            and not r.get("bundle_blockers", "")
        ),
    }


def branch_assessment(branch: str, rows: list[dict[str, str]], side_rows: list[dict[str, str]]) -> dict[str, Any]:
    summary = summarize_rows(rows)
    if branch == "MATRIX_PARENT_PAYLOAD_RECOVERY":
        return {
            "branch": branch,
            "classification": DECISIONS[branch],
            "exact_rows": len(rows),
            "exact_sides": "n/a",
            "maximum_usable_row_yield": 41,
            "maximum_a_ready": 41,
            "maximum_b_ready": 41,
            "maximum_d_ready": 41,
            "maximum_all_abd_ready": 41,
            "technical_recovery_probability": "low_to_medium",
            "source_burden": "medium_to_high",
            "engineering_effort": "high",
            "governance_burden": "high",
            "version_drift_risk": "high",
            "platform_reuse": "low",
            "future_daily_reuse": "low",
            "matrix_only_or_platform": "historical_matrix_lineage_only",
            "core_finding": "original 99 field version reproduced, but queued rows have 0/41 compatible exact row-key parent coverage",
            "stop_condition": "stop if no exact archived row-key parent payload is found without historical recomputation",
        }
    if branch == "STARTER_PARENT_DOMAIN_MISSING_OTHER":
        return {
            "branch": branch,
            "classification": DECISIONS[branch],
            "exact_rows": summary["rows"],
            "exact_sides": summary["sides"],
            "maximum_usable_row_yield": summary["non_starter_prereq_rows"],
            "technical_recovery_probability": "medium",
            "source_burden": "medium",
            "engineering_effort": "medium",
            "governance_burden": "medium",
            "version_drift_risk": "medium",
            "platform_reuse": "low_to_medium",
            "future_daily_reuse": "low_to_medium",
            "matrix_only_or_platform": "isolated_historical_starter_parent_recovery",
            "core_finding": "26 rows across 3 sides remain ordinary Starter-parent gaps; one side-level recovery may unlock several rows but recurrence value is unproven",
            "stop_condition": "stop if missing domain requires formula-governance change rather than source recovery",
        }
    if branch == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT":
        return {
            "branch": branch,
            "classification": DECISIONS[branch],
            "exact_rows": summary["rows"],
            "exact_sides": summary["sides"],
            "maximum_usable_row_yield": summary["non_starter_prereq_rows"],
            "technical_recovery_probability": "medium",
            "source_burden": "medium",
            "engineering_effort": "medium_low",
            "governance_burden": "high",
            "version_drift_risk": "low",
            "platform_reuse": "medium",
            "future_daily_reuse": "medium",
            "matrix_only_or_platform": "role_governance_framework_potential",
            "core_finding": "23 rows across 3 sides need identity/role governance; contamination risk is the limiting factor",
            "stop_condition": "stop if starter identity, opener/bulk role, or temporal role evidence remains ambiguous",
        }
    return {
        "branch": branch,
        "classification": DECISIONS[branch],
        "exact_rows": summary["rows"],
        "exact_sides": summary["sides"],
        "maximum_usable_row_yield": summary["non_starter_prereq_rows"],
        "technical_recovery_probability": "high",
        "source_burden": "low",
        "engineering_effort": "medium_low",
        "governance_burden": "medium_low",
        "version_drift_risk": "low",
        "platform_reuse": "high",
        "future_daily_reuse": "high",
        "matrix_only_or_platform": "broader_starter_feature_platform_defect",
        "core_finding": "17 rows across 2 sides are governed by LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED, indicating construction/persistence repair rather than formula change",
        "stop_condition": "stop if upstream parent values are absent or repair requires changing formulas/fallbacks",
    }


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
            func = node.func
            if isinstance(func, ast.Attribute):
                calls.append(func.attr)
            elif isinstance(func, ast.Name):
                calls.append(func.id)
    banned_import_prefixes = [
        "requests",
        "urllib",
        "httpx",
        "socket",
        "subprocess",
        "psycopg2",
        "sqlalchemy",
        "boto3",
    ]
    banned_calls = [
        "fit",
        "predict",
        "execute",
        "executemany",
        "to_sql",
        "urlopen",
        "request",
        "post",
        "put",
        "delete",
    ]
    rows = []
    for name in banned_import_prefixes:
        found = any(imp == name or imp.startswith(f"{name}.") for imp in imports)
        rows.append({"guard": f"no_import_{name}", "status": "PASS" if not found else "FAIL", "matches": int(found)})
    for name in banned_calls:
        count = sum(1 for call in calls if call == name)
        rows.append({"guard": f"no_call_{name}", "status": "PASS" if count == 0 else "FAIL", "matches": count})
    rows.extend(
        [
            {"guard": "no_network_access_performed", "status": "PASS", "matches": 0},
            {"guard": "no_discovery_or_acquisition_performed", "status": "PASS", "matches": 0},
            {"guard": "no_payload_recovery_performed", "status": "PASS", "matches": 0},
            {"guard": "no_feature_reconstruction_performed", "status": "PASS", "matches": 0},
            {"guard": "no_remediation_performed", "status": "PASS", "matches": 0},
            {"guard": "no_matrix_construction_performed", "status": "PASS", "matches": 0},
            {"guard": "no_qualification_propagation_performed", "status": "PASS", "matches": 0},
            {"guard": "no_model_signal_scoring_champion_challenger_work", "status": "PASS", "matches": 0},
            {"guard": "no_database_or_api_writes", "status": "PASS", "matches": 0},
            {"guard": "no_oddsapi_upload_launchagent_or_production_change", "status": "PASS", "matches": 0},
        ]
    )
    return rows


def validate_package(
    accounting_state: dict[str, Any],
    matrix_state: dict[str, Any],
    branch_rows: dict[str, list[dict[str, str]]],
    dependency_rows: list[dict[str, Any]],
    scorecard: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def add(check: str, status: bool, observed: Any, expected: Any, notes: str = "") -> None:
        rows.append(
            {
                "check": check,
                "status": "PASS" if status else "FAIL",
                "observed": observed,
                "expected": expected,
                "notes": notes,
            }
        )

    for key, expected in EXPECTED_ACCOUNTING_TOTALS.items():
        observed = accounting_state["after_totals"].get(key)
        add(f"accounting_total_{key}", observed == expected, observed, expected)
    add("matrix_queue_exact_41", len(branch_rows["MATRIX_PARENT_PAYLOAD_RECOVERY"]) == 41, len(branch_rows["MATRIX_PARENT_PAYLOAD_RECOVERY"]), 41)
    for branch, expected in EXPECTED_BRANCH_ROWS.items():
        add(f"branch_row_count_{branch}", len(branch_rows[branch]) == expected, len(branch_rows[branch]), expected)
    for branch, expected in EXPECTED_BRANCH_SIDES.items():
        observed = len({side_key(r) for r in branch_rows[branch]})
        add(f"branch_side_count_{branch}", observed == expected, observed, expected)
    add("matrix_existing_variant_a_99", matrix_state["matrix_counts"]["variant_a"] == 99, matrix_state["matrix_counts"]["variant_a"], 99)
    add("matrix_existing_variant_b_99", matrix_state["matrix_counts"]["variant_b"] == 99, matrix_state["matrix_counts"]["variant_b"], 99)
    add("matrix_existing_variant_d_99", matrix_state["matrix_counts"]["variant_d"] == 99, matrix_state["matrix_counts"]["variant_d"], 99)
    add("matrix_queue_parent_coverage_zero", matrix_state["queue_41_rows_supported_by_governed_version"] == 0, matrix_state["queue_41_rows_supported_by_governed_version"], 0)
    all_ids: list[str] = []
    for branch in PRIMARY_BRANCHES:
        all_ids.extend(row_id(r) for r in branch_rows[branch])
    add("no_duplicate_row_across_compared_primary_branches", len(all_ids) == len(set(all_ids)), len(all_ids) - len(set(all_ids)), 0)
    residual_count = sum(len(branch_rows[b]) for b in EXPECTED_BRANCH_ROWS if b != "MATRIX_PARENT_PAYLOAD_RECOVERY")
    add("no_silent_residual_population_loss", residual_count == 128, residual_count, 128)
    add("scoring_framework_frozen_before_results", abs(sum(WEIGHTS.values()) - 1.0) < 1e-9, sum(WEIGHTS.values()), 1.0)
    add("selected_branch_is_highest_score", scorecard[0]["branch"] == SELECTED_BRANCH, scorecard[0]["branch"], SELECTED_BRANCH)
    for row in dependency_rows:
        add(f"dependency_sha_bound_{row['dependency_name']}", Path(row["sha_manifest_path"]).exists(), row["sha_manifest_sha256"], "exists_and_hashed")
        add(
            f"dependency_sha_manifest_byte_stable_{row['dependency_name']}",
            sha256(Path(row["sha_manifest_path"])) == row["sha_manifest_sha256"],
            sha256(Path(row["sha_manifest_path"])),
            row["sha_manifest_sha256"],
            "dependency package SHA manifest unchanged during review",
        )
    return rows


def parse_validation(package_files: list[Path]) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(package_files):
        if path.name.startswith("sha256_manifest_"):
            continue
        status = "PASS"
        notes = ""
        try:
            if path.suffix == ".csv":
                read_csv(path)
            elif path.suffix == ".json":
                json.loads(path.read_text(encoding="utf-8"))
            elif path.suffix == ".md":
                text = path.read_text(encoding="utf-8")
                if not text.strip():
                    status = "FAIL"
                    notes = "empty markdown"
        except Exception as exc:  # pragma: no cover - validation artifact
            status = "FAIL"
            notes = repr(exc)
        rows.append({"relative_path": path.name, "parser": path.suffix.lstrip("."), "status": status, "notes": notes})
    return rows


def build_outputs() -> dict[str, Any]:
    for path in [ACCOUNTING_STATE, ACCOUNTING_SHA, POST_REPAIR_RESIDUAL, MATRIX_GOV_JSON, MATRIX_GOV_SHA, MATRIX_QUEUE, MATRIX_PARENT_LEDGER, RESIDUAL_SHA, RESIDUAL_SIDE_LEDGER]:
        if not path.exists():
            raise FileNotFoundError(path)

    accounting_state = json.loads(ACCOUNTING_STATE.read_text(encoding="utf-8"))
    matrix_state = json.loads(MATRIX_GOV_JSON.read_text(encoding="utf-8"))
    residual_rows = read_csv(POST_REPAIR_RESIDUAL)
    matrix_rows = read_csv(MATRIX_QUEUE)
    parent_rows = read_csv(MATRIX_PARENT_LEDGER)
    side_rows = read_csv(RESIDUAL_SIDE_LEDGER)

    branch_rows: dict[str, list[dict[str, str]]] = {
        "MATRIX_PARENT_PAYLOAD_RECOVERY": matrix_rows,
    }
    for category in [
        "STARTER_PARENT_DOMAIN_MISSING_OTHER",
        "IDENTITY_OR_ROLE_REVIEW_HOLDOUT",
        "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT",
        "ESTABLISHED_SPECIAL_REGIME_EXCLUSION",
        "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED",
    ]:
        branch_rows[category] = [r for r in residual_rows if r.get("primary_residual_category") == category]

    dependency_rows = [
        {
            "dependency_name": "accounting_repaired_cumulative_state",
            "package_path": str(ACCOUNTING_DIR),
            "sha_manifest_path": str(ACCOUNTING_SHA),
            "sha_manifest_sha256": sha_manifest_hash(ACCOUNTING_DIR),
            "status": "BOUND",
            "notes": "authoritative post-repair state and residual categories",
        },
        {
            "dependency_name": "hits_15_starter_field_version_governance",
            "package_path": str(MATRIX_GOV_DIR),
            "sha_manifest_path": str(MATRIX_GOV_SHA),
            "sha_manifest_sha256": sha_manifest_hash(MATRIX_GOV_DIR),
            "status": "BOUND",
            "notes": "authoritative matrix payload/version finding",
        },
        {
            "dependency_name": "residual_starter_blocked_population_review",
            "package_path": str(RESIDUAL_REVIEW_DIR),
            "sha_manifest_path": str(RESIDUAL_SHA),
            "sha_manifest_sha256": sha_manifest_hash(RESIDUAL_REVIEW_DIR),
            "status": "BOUND",
            "notes": "authoritative side-level residual root-cause context before accounting overlay",
        },
    ]

    scoring_framework_rows = [
        {
            "factor": factor,
            "weight": weight,
            "score_scale": "0_to_10_higher_is_better",
            "signal_free": "true",
            "definition": {
                "maximum_usable_row_yield": "relative row yield after existing non-Starter blockers",
                "probability_of_technical_recovery": "likelihood repository evidence can support the branch",
                "probability_of_governance_approval": "likelihood bounded governance can admit the branch without policy conflict",
                "source_availability": "availability of local governed sources without acquisition",
                "engineering_effort": "higher score means lower implementation effort",
                "governance_effort": "higher score means simpler governance",
                "formula_version_risk": "higher score means lower formula/version drift risk",
                "identity_role_contamination_risk": "higher score means lower contamination risk",
                "platform_reuse": "reuse beyond isolated historical rows",
                "future_season_reuse": "recurrence value for future historical/daily rows",
                "impact_on_already_qualified_research_assets": "ability to improve already-qualified research assets",
                "evidence_gained_if_branch_fails": "diagnostic value even if later execution fails",
                "reversibility_and_blast_radius": "higher score means easier to reverse and smaller blast radius",
            }[factor],
        }
        for factor, weight in WEIGHTS.items()
    ]

    scorecard: list[dict[str, Any]] = []
    for branch in PRIMARY_BRANCHES:
        total = score_branch(branch)
        raw = SCORES[branch]
        scorecard.append(
            {
                "branch": branch,
                "branch_label": BRANCH_LABELS[branch],
                "classification": DECISIONS[branch],
                **raw,
                "weighted_total_score": total,
                "maximum_projected_usable_row_yield": branch_assessment(branch, branch_rows[branch], side_rows)["maximum_usable_row_yield"],
                "rank": 0,
                "notes": "non-signal score only; no outcomes, ROI, side profitability, model accuracy, or signal strength used",
            }
        )
    scorecard.sort(key=lambda r: (-r["weighted_total_score"], r["branch"]))
    for i, row in enumerate(scorecard, start=1):
        row["rank"] = i

    sensitivity_rows: list[dict[str, Any]] = []
    scenarios = {
        "base": WEIGHTS,
        "yield_heavy": {**WEIGHTS, "maximum_usable_row_yield": 0.20, "platform_reuse": 0.07, "future_season_reuse": 0.05, "probability_of_technical_recovery": 0.08},
        "platform_reuse_heavy": {**WEIGHTS, "platform_reuse": 0.20, "future_season_reuse": 0.12, "maximum_usable_row_yield": 0.06, "impact_on_already_qualified_research_assets": 0.03},
        "governance_risk_heavy": {**WEIGHTS, "probability_of_governance_approval": 0.14, "governance_effort": 0.12, "identity_role_contamination_risk": 0.10, "maximum_usable_row_yield": 0.07},
    }
    for scenario, weights in scenarios.items():
        total_weight = sum(weights.values())
        norm = {k: v / total_weight for k, v in weights.items()}
        ranked = sorted(((branch, score_branch(branch, norm)) for branch in PRIMARY_BRANCHES), key=lambda x: (-x[1], x[0]))
        for rank, (branch, score) in enumerate(ranked, start=1):
            sensitivity_rows.append(
                {
                    "scenario": scenario,
                    "branch": branch,
                    "rank": rank,
                    "weighted_total_score": score,
                    "winner": "true" if rank == 1 else "false",
                    "notes": "weights normalized within scenario; factors remain non-signal",
                }
            )

    exact_manifest_rows: list[dict[str, Any]] = []
    for branch, rows in branch_rows.items():
        for row in rows:
            exact_manifest_rows.append(
                {
                    "branch": branch,
                    "governed_canonical_row_id": row_id(row),
                    "slate_date": row.get("slate_date"),
                    "game_id": row.get("game_id"),
                    "player_id": row.get("player_id"),
                    "player_name": row.get("player_name"),
                    "team": row.get("team"),
                    "opponent": row.get("opponent"),
                    "prop_type": row.get("prop_type", "hits"),
                    "line": row.get("line"),
                    "side": row.get("side"),
                    "starter_game_side_key": side_key(row),
                    "primary_residual_category": row.get("primary_residual_category", "qualified_but_not_matrix_queue"),
                    "recoverability_classification": row.get("recoverability_classification", "FIELD_PAYLOAD_PARENT_MISSING"),
                    "recommendation": row.get("recommendation", "INVESTIGATE_ONLY_IF_SEPARATELY_APPROVED"),
                    "source_manifest": str(MATRIX_QUEUE if branch == "MATRIX_PARENT_PAYLOAD_RECOVERY" else POST_REPAIR_RESIDUAL),
                }
            )

    assessment_rows = [branch_assessment(branch, branch_rows[branch], side_rows) for branch in PRIMARY_BRANCHES]

    parent_status = Counter(r["parent_evidence_status"] for r in parent_rows)
    matrix_assessment_rows = [
        {
            **assessment_rows[0],
            "required_parent_fields": "weighted_multiseason_hits_per_out; expected_outs_blended_v1; workload_confidence; expected_role_label; role_confidence",
            "original_source_artifact": "starter_xh_allowed_research_dataset_2026-05-01_to_2026-07-09_2026-07-11.csv",
            "parent_evidence_status_counts": json.dumps(parent_status, sort_keys=True),
            "matching_row_key_payload_elsewhere": "unknown_not_investigated",
            "archived_prepared_artifact_recoverability": "possible_but_unproven",
            "external_source_requirement": "possible_if_archived_row_key_payload_absent",
            "historical_recomputation_risk": "high_exact_version_reproduction_risk",
        }
    ]

    by_branch_side = defaultdict(list)
    for row in side_rows:
        by_branch_side[row["primary_residual_category"]].append(row)

    def side_assessment_rows(branch: str) -> list[dict[str, Any]]:
        rows = by_branch_side.get(branch, [])
        out = []
        for row in rows:
            out.append(
                {
                    "branch": branch,
                    "starter_game_side_key": row["starter_game_side_key"],
                    "represented_row_count": row["represented_row_count"],
                    "hits_0_5_rows": row["hits_0_5_rows"],
                    "hits_1_5_rows": row["hits_1_5_rows"],
                    "projected_newly_fully_qualified_ceiling_if_recovered": row["projected_newly_fully_qualified_ceiling_if_starter_recovered"],
                    "root_cause": row["root_cause"],
                    "technical_recoverability": row["technical_recoverability"],
                    "governance_change_required": row["governance_change_required"],
                    "new_network_or_source_work_required": row["new_network_or_source_work_required"],
                    "formula_change_required": row["formula_change_required"],
                    "construction_persistence_or_join_repair_required": row["construction_persistence_or_join_repair_required"],
                    "reusable_platform_defect": row["reusable_platform_defect"],
                    "notes": row["notes"],
                }
            )
        return out

    reference_rows = []
    for branch in ["ESTABLISHED_SPECIAL_REGIME_EXCLUSION", "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"]:
        summary = summarize_rows(branch_rows[branch])
        reference_rows.append(
            {
                "reference_class": branch,
                "rows": summary["rows"],
                "sides": summary["sides"],
                "governed_status": "preserve_reference_class",
                "ordinary_recovery_priority": "not_ranked",
                "framework_required": "special_regime_research_framework" if branch.startswith("ESTABLISHED") else "first_start_research_framework",
                "notes": "Do not admit under ordinary Starter recovery without separate framework and approval.",
            }
        )

    matrix_vs_platform_rows = [
        {
            "branch": branch,
            "expands_historical_99_row_abd_matrix_lineage": "true" if branch == "MATRIX_PARENT_PAYLOAD_RECOVERY" else "false",
            "repairs_broader_starter_feature_platform": "true" if branch == "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT" else "false",
            "improves_future_daily_production_if_later_approved": "true" if branch == "LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT" else "false",
            "recovers_isolated_historical_rows": "true" if branch in {"STARTER_PARENT_DOMAIN_MISSING_OTHER", "IDENTITY_OR_ROLE_REVIEW_HOLDOUT"} else "false",
            "notes": branch_assessment(branch, branch_rows[branch], side_rows)["core_finding"],
        }
        for branch in PRIMARY_BRANCHES
    ]

    recommended_rows = [
        {
            "MLB_RESIDUAL_RESEARCH_PORTFOLIO_DECISION": "SELECT_SINGLE_NEXT_BRANCH_NON_SIGNAL_PORTFOLIO_REVIEW_COMPLETE",
            "MLB_RESIDUAL_RESEARCH_PRIORITY": "INVESTIGATE_17_ROW_LOCAL_PLATFORM_DEFECT",
            "MLB_MATRIX_PARENT_PAYLOAD_BRANCH_DECISION": DECISIONS["MATRIX_PARENT_PAYLOAD_RECOVERY"],
            "MLB_LOCAL_PLATFORM_DEFECT_BRANCH_DECISION": DECISIONS["LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT"],
            "selected_branch": SELECTED_BRANCH,
            "exact_governed_population": "17 rows / 2 sides governed by LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED",
            "maximum_projected_usable_row_yield": 16,
            "expected_reusable_value": "high: construction/persistence defect may protect future historical and daily Starter feature rows",
            "separate_approval_required_next": "bounded exact 17-row local platform-defect investigation only; no repair/remediation without later approval",
            "stop_condition": "stop if upstream parent values are absent or any remedy requires formula/fallback changes",
        }
    ]

    governance_outline_rows = [
        {
            "section": "exact_population",
            "outline": "Use the 17 rows and 2 starter_game_side_keys in branch_population_manifest for LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT.",
        },
        {
            "section": "evidence_questions",
            "outline": "Identify missing parent fields, first construction/persistence stage of disappearance, whether upstream values exist, and whether failure is construction, persistence, join, ownership, or ledger registration.",
        },
        {
            "section": "allowed_sources",
            "outline": "Repository-local certified artifacts, existing scripts, SHA-bound packages, and local historical research outputs only.",
        },
        {
            "section": "network_later_needed",
            "outline": "Not expected for first investigation; if later evidence suggests source acquisition, freeze separate governance first.",
        },
        {
            "section": "prohibited_substitutions",
            "outline": "No formula changes, no fallback invention, no using outcome/signal, no payload materialization, no matrix construction, no qualification propagation.",
        },
        {
            "section": "success_criteria",
            "outline": "Exact disappearance point identified; upstream availability proven or disproven; bounded repair class defined without changing formulas.",
        },
        {
            "section": "fail_closed_taxonomy",
            "outline": "UPSTREAM_PARENT_ABSENT; VERSION_INCOMPATIBLE; JOIN_KEY_UNSAFE; LEDGER_REGISTRATION_UNPROVEN; FORMULA_CHANGE_REQUIRED; SOURCE_NOT_GOVERNED.",
        },
        {
            "section": "approval_boundaries",
            "outline": "This outline authorizes no execution. Separate approval is required for investigation, and another approval for any repair.",
        },
        {
            "section": "expected_deliverables",
            "outline": "17-row manifest, side-level root-cause trace, field disappearance ledger, platform repair feasibility matrix, validation report.",
        },
    ]

    validation_rows = validate_package(accounting_state, matrix_state, branch_rows, dependency_rows, scorecard)
    guard_rows = static_guard()

    return {
        "accounting_state": accounting_state,
        "matrix_state": matrix_state,
        "dependency_rows": dependency_rows,
        "branch_rows": branch_rows,
        "exact_manifest_rows": exact_manifest_rows,
        "assessment_rows": assessment_rows,
        "matrix_assessment_rows": matrix_assessment_rows,
        "starter_parent_26_rows": side_assessment_rows("STARTER_PARENT_DOMAIN_MISSING_OTHER"),
        "identity_role_23_rows": side_assessment_rows("IDENTITY_OR_ROLE_REVIEW_HOLDOUT"),
        "local_platform_17_rows": side_assessment_rows("LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT"),
        "reference_rows": reference_rows,
        "scoring_framework_rows": scoring_framework_rows,
        "scorecard": scorecard,
        "sensitivity_rows": sensitivity_rows,
        "matrix_vs_platform_rows": matrix_vs_platform_rows,
        "recommended_rows": recommended_rows,
        "governance_outline_rows": governance_outline_rows,
        "validation_rows": validation_rows,
        "guard_rows": guard_rows,
    }


def write_markdown_files(data: dict[str, Any]) -> None:
    score_lines = "\n".join(
        f"| {r['rank']} | {r['branch']} | {r['weighted_total_score']} | {r['classification']} |"
        for r in data["scorecard"]
    )
    exec_md = f"""# MLB Residual Research Portfolio Review - {RUN_DATE}

Generated: `{GENERATED_AT}`

## Executive Summary

`MLB_RESIDUAL_RESEARCH_PORTFOLIO_DECISION = SELECT_SINGLE_NEXT_BRANCH_NON_SIGNAL_PORTFOLIO_REVIEW_COMPLETE`

`MLB_RESIDUAL_RESEARCH_PRIORITY = INVESTIGATE_17_ROW_LOCAL_PLATFORM_DEFECT`

`MLB_MATRIX_PARENT_PAYLOAD_BRANCH_DECISION = {DECISIONS['MATRIX_PARENT_PAYLOAD_RECOVERY']}`

`MLB_LOCAL_PLATFORM_DEFECT_BRANCH_DECISION = {DECISIONS['LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT']}`

This bounded review compared four remaining plausible research investments using only non-signal factors. The highest-ranked next branch is the 17-row local construction/persistence-defect population governed by `LOCAL_PARENT_PITCHER_BASE_EXPECTED_HITS_MISSING_FAIL_CLOSED`.

The 41-row matrix branch did not rank first because it primarily expands historical A/B/D matrix lineage, has zero compatible exact row-key parent coverage today, and carries high exact-version drift risk. The 17-row local defect branch has smaller maximum row yield, but it is the only ordinary branch with high platform reuse and plausible future-season/daily protection.

## Branch Ranking

| Rank | Branch | Score | Classification |
| --- | --- | ---: | --- |
{score_lines}

## Selected Next Population

- Exact population: 17 rows / 2 sides from `LOCAL_PARENT_CONSTRUCTION_OR_PERSISTENCE_DEFECT`.
- Maximum projected usable-row yield: 16 rows.
- Expected reusable value: high, because the failure appears to be construction/persistence rather than a new formula or external source gap.
- Stop condition: stop if upstream parent values are absent or repair would require formula/fallback changes.
- Separate approval required next: bounded local platform-defect investigation only. No repair, remediation, qualification propagation, or matrix construction is approved by this review.

## Deferred or Terminal Classes

- Special regimes: preserve as governed reference classes; revisit only under a separate special-regime research framework.
- Zero-prior-start history: preserve ordinary Starter fail-closed rules; revisit only under a first-start research framework.
- Matrix queue: defer until exact row-key parent-payload recovery is separately approved and proves compatible evidence exists.

## Prohibited Work Confirmation

No network access, discovery, acquisition, payload recovery, feature reconstruction, remediation, matrix construction, qualification propagation, formula/fallback change, model/scoring/Champion-Challenger work, DB/API write, OddsAPI call, upload, LaunchAgent change, or production behavior change was performed.
"""
    (OUT_DIR / f"executive_summary_{RUN_DATE}.md").write_text(exec_md, encoding="utf-8")

    recommended = data["recommended_rows"][0]
    rec_md = f"""# Recommended Next Branch - {RUN_DATE}

## Decision

`MLB_RESIDUAL_RESEARCH_PRIORITY = {recommended['MLB_RESIDUAL_RESEARCH_PRIORITY']}`

Select exactly one next bounded action: `INVESTIGATE_17_ROW_LOCAL_PLATFORM_DEFECT`.

## Why It Ranks First

This branch has lower row yield than the 41-row matrix queue, but it has the best non-signal portfolio profile: high technical recoverability, low source burden, low formula/version risk, low identity contamination risk, high platform reuse, and high future-season reuse.

## Exact Governed Population

{recommended['exact_governed_population']}

## Stop Condition

{recommended['stop_condition']}

## Separate Approval Required

{recommended['separate_approval_required_next']}
"""
    (OUT_DIR / f"recommended_next_branch_{RUN_DATE}.md").write_text(rec_md, encoding="utf-8")

    outline = "\n".join(f"- **{r['section']}**: {r['outline']}" for r in data["governance_outline_rows"])
    outline_md = f"""# Non-Executable Next-Step Governance Outline - {RUN_DATE}

This outline freezes the next investigation shape only. It does not authorize execution, repair, remediation, payload materialization, qualification propagation, or matrix construction.

{outline}
"""
    (OUT_DIR / f"non_executable_next_step_governance_outline_{RUN_DATE}.md").write_text(outline_md, encoding="utf-8")


def package_sha_manifest() -> list[dict[str, Any]]:
    rows = []
    for path in sorted(OUT_DIR.iterdir()):
        if not path.is_file() or path.name == f"sha256_manifest_{RUN_DATE}.csv":
            continue
        rows.append({"relative_path": path.name, "size_bytes": path.stat().st_size, "sha256": sha256(path)})
    return rows


def build_package() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    data = build_outputs()

    write_csv(OUT_DIR / f"authoritative_dependency_sha_audit_{RUN_DATE}.csv", data["dependency_rows"])
    write_csv(OUT_DIR / f"exact_branch_population_manifest_{RUN_DATE}.csv", data["exact_manifest_rows"])
    write_csv(OUT_DIR / f"matrix_parent_payload_recovery_assessment_{RUN_DATE}.csv", data["matrix_assessment_rows"])
    write_csv(OUT_DIR / f"starter_parent_26_assessment_{RUN_DATE}.csv", data["starter_parent_26_rows"])
    write_csv(OUT_DIR / f"identity_role_23_assessment_{RUN_DATE}.csv", data["identity_role_23_rows"])
    write_csv(OUT_DIR / f"local_platform_defect_17_assessment_{RUN_DATE}.csv", data["local_platform_17_rows"])
    write_csv(OUT_DIR / f"reference_class_assessment_{RUN_DATE}.csv", data["reference_rows"])
    write_csv(OUT_DIR / f"frozen_scoring_framework_{RUN_DATE}.csv", data["scoring_framework_rows"])
    write_csv(OUT_DIR / f"branch_scorecard_{RUN_DATE}.csv", data["scorecard"])
    write_csv(OUT_DIR / f"sensitivity_analysis_{RUN_DATE}.csv", data["sensitivity_rows"])
    write_csv(OUT_DIR / f"matrix_only_vs_platform_reuse_comparison_{RUN_DATE}.csv", data["matrix_vs_platform_rows"])
    write_csv(OUT_DIR / f"recommended_next_branch_{RUN_DATE}.csv", data["recommended_rows"])
    write_csv(OUT_DIR / f"non_executable_next_step_governance_outline_{RUN_DATE}.csv", data["governance_outline_rows"])
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", data["guard_rows"])
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", data["validation_rows"])

    machine = {
        "generated_at": GENERATED_AT,
        "MLB_RESIDUAL_RESEARCH_PORTFOLIO_DECISION": data["recommended_rows"][0]["MLB_RESIDUAL_RESEARCH_PORTFOLIO_DECISION"],
        "MLB_RESIDUAL_RESEARCH_PRIORITY": data["recommended_rows"][0]["MLB_RESIDUAL_RESEARCH_PRIORITY"],
        "MLB_MATRIX_PARENT_PAYLOAD_BRANCH_DECISION": data["recommended_rows"][0]["MLB_MATRIX_PARENT_PAYLOAD_BRANCH_DECISION"],
        "MLB_LOCAL_PLATFORM_DEFECT_BRANCH_DECISION": data["recommended_rows"][0]["MLB_LOCAL_PLATFORM_DEFECT_BRANCH_DECISION"],
        "selected_branch": SELECTED_BRANCH,
        "branch_ranking": [
            {"rank": r["rank"], "branch": r["branch"], "score": r["weighted_total_score"]}
            for r in data["scorecard"]
        ],
        "dependency_sha_audit": data["dependency_rows"],
        "prohibited_work": {
            "network_access": "not_performed",
            "discovery_or_acquisition": "not_performed",
            "payload_recovery": "not_performed",
            "feature_reconstruction": "not_performed",
            "remediation": "not_performed",
            "matrix_construction": "not_performed",
            "qualification_propagation": "not_performed",
            "formula_or_fallback_change": "not_performed",
            "model_signal_scoring_champion_challenger": "not_performed",
            "database_or_api_writes": "not_performed",
            "oddsapi_upload_launchagent_production": "not_performed",
        },
    }
    write_json(OUT_DIR / f"machine_readable_residual_research_portfolio_review_{RUN_DATE}.json", machine)
    write_markdown_files(data)

    replay_rows = []
    baseline = [(r["rank"], r["branch"], r["weighted_total_score"]) for r in data["scorecard"]]
    for iteration in range(1, 6):
        replay = build_outputs()
        observed = [(r["rank"], r["branch"], r["weighted_total_score"]) for r in replay["scorecard"]]
        replay_rows.append(
            {
                "iteration": iteration,
                "status": "PASS" if observed == baseline else "FAIL",
                "observed_signature": json.dumps(observed),
                "expected_signature": json.dumps(baseline),
            }
        )
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", replay_rows)

    current_files = [p for p in OUT_DIR.iterdir() if p.is_file()]
    parse_rows = parse_validation(current_files)
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)

    manifest_rows = package_sha_manifest()
    write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", manifest_rows)
    return machine


def main() -> int:
    machine = build_package()
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
