#!/usr/bin/env python3
"""Reconcile and freeze cumulative-state governance for DISCOVERY_COHORT_004.

Governance reconciliation only. This utility re-anchors the existing
DISCOVERY_COHORT_004 plan to the certified post-COHORT_003 cumulative campaign
state. It does not execute discovery, acquisition, reconstruction, remediation,
qualification propagation, matrix construction, model/scoring work, database/API
writes, uploads, scheduler edits, or production behavior changes.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter
from pathlib import Path
from typing import Any


RUN_DATE = "2026-07-15"
GENERATED_AT = "2026-07-15T00:00:00+00:00"
COHORT_ID = "DISCOVERY_COHORT_004"

DECISION = (
    "STARTER_DISCOVERY_COHORT_004_CUMULATIVE_GOVERNANCE_DECISION = "
    "CUMULATIVE_STATE_RECONCILED_EXISTING_COHORT_FROZEN_UNCHANGED"
)
STATUS = (
    "STARTER_DISCOVERY_COHORT_004_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_BOUNDED_DISCOVERY_APPROVAL"
)

EXPECTED_PARENT_SHA = "d7629fab6efcb3b48a1432323aa861c0ae7390a00595430226471b2129123856"
EXPECTED_SCALE_SHA = "f6ead8dfc5482b89ee9bdd349c6538dd9d1430c704c489a40e65b4664d02d33c"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_004_cumulative_state_governance/2026-07-15"
)
PARENT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/2026-07-15"
)
SCALE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/2026-07-15"
)
FOUR_SIDE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/2026-07-15"
)
HC_LOCAL_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/2026-07-15"
)
COHORT_DIRS = {
    "discovery_cohort_001": Path(
        "artifacts/analysis/model_development/"
        "mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_remediation/2026-07-15"
    ),
    "discovery_cohort_002": Path(
        "artifacts/analysis/model_development/"
        "mlb_historical_selected_proposition_discovery_cohort_002_starter_reconstruction_remediation/2026-07-15"
    ),
    "discovery_cohort_003": Path(
        "artifacts/analysis/model_development/"
        "mlb_historical_selected_proposition_discovery_cohort_003_starter_reconstruction_remediation/2026-07-15"
    ),
}
TARGET_MANIFEST_DIRS = {
    "discovery_cohort_001": Path(
        "artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_001/2026-07-15"
    ),
    "discovery_cohort_002": Path(
        "artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_002/2026-07-15"
    ),
    "discovery_cohort_003": Path(
        "artifacts/analysis/model_development/mlb_historical_selected_proposition_discovery_cohort_003/2026-07-15"
    ),
}
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)
MATRIX_PATHS = [
    MATRIX_DIR / "variant_a_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_b_hits_1_5_qualified_matrix_2026-07-14.csv",
    MATRIX_DIR / "variant_d_hits_1_5_qualified_matrix_2026-07-14.csv",
]

PARENT_STATE = PARENT_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"
SCALE_PLAN = SCALE_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv"
SCALE_REMAINING_SIDES = SCALE_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv"
SCALE_803 = SCALE_DIR / f"authoritative_803_row_campaign_reconciliation_{RUN_DATE}.csv"
SCALE_96 = SCALE_DIR / f"authoritative_96_side_campaign_reconciliation_{RUN_DATE}.csv"
ROLE_HOLDOUTS = SCALE_DIR / f"held_out_identity_role_review_ledger_{RUN_DATE}.csv"
DOWNSTREAM_LIMITED = SCALE_DIR / f"held_out_downstream_limited_ledger_{RUN_DATE}.csv"

PROHIBITED_PATTERNS = {
    "network_or_source_acquisition": re.compile(r"requests[.]|httpx|urlopen|urlretrieve|download", re.IGNORECASE),
    "training_prediction_or_matrix": re.compile(
        r"[.]fit\s*[(]|[.]predict\s*[(]|roc_auc|log_loss|build_mlb_selected_proposition_abd_matrices",
        re.IGNORECASE,
    ),
    "db_api_write": re.compile(
        r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*[(])\b",
        re.IGNORECASE,
    ),
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


def package_sha(path: Path, date_value: str = RUN_DATE) -> str:
    return sha256_path(path / f"sha256_manifest_{date_value}.csv")


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def boolish(value: Any) -> bool:
    return str(value).strip().lower() == "true"


def row_id(row: dict[str, str]) -> str:
    return row.get("governed_canonical_row_id") or row.get("canonical_denominator_identity") or row.get("canonical_row_id", "")


def side_from_movement(row: dict[str, str]) -> str:
    return row.get("governed_starter_game_side_identity") or row.get("starter_game_side_key", "")


def split_row_id(value: str) -> dict[str, str]:
    parts = value.split("|")
    if len(parts) >= 6:
        return {
            "slate_date": parts[0],
            "game_id": parts[1],
            "player_id": parts[2],
            "prop_type": parts[3],
            "line": parts[4],
            "side": parts[5],
        }
    return {"slate_date": "", "game_id": "", "player_id": "", "prop_type": "", "line": "", "side": ""}


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
        rows.append(
            {
                "check": name,
                "status": "PASS" if not matches else "FAIL",
                "matches": "|".join(str(m) for m in matches),
                "notes": "Static guard excludes comments/string literals and scans executable code only.",
            }
        )
    return rows


class Cohort004CumulativeGovernance:
    def __init__(self) -> None:
        self.parent_state = json.loads(PARENT_STATE.read_text(encoding="utf-8"))
        self.scale_plan = read_csv(SCALE_PLAN)
        self.remaining_sides = read_csv(SCALE_REMAINING_SIDES)
        self.rows_803 = read_csv(SCALE_803)
        self.sides_96 = read_csv(SCALE_96)
        self.role_holdouts = read_csv(ROLE_HOLDOUTS)
        self.downstream_limited = read_csv(DOWNSTREAM_LIMITED)
        self.cohort_plan = next(row for row in self.scale_plan if row["cohort_id"] == COHORT_ID)
        self.cohort_side_keys = self.cohort_plan["side_keys"].split(";")
        self.side_by_key = {row["starter_game_side_key"]: row for row in self.remaining_sides}
        self.cohort_sides = [self.side_by_key[key] for key in self.cohort_side_keys]
        self.cohort_side_set = set(self.cohort_side_keys)
        self.cohort_rows = [row for row in self.rows_803 if row["starter_game_side_key"] in self.cohort_side_set]
        self.cohort_row_ids = {row_id(row) for row in self.cohort_rows}
        self.cohort_targets = {row["expected_discovery_key"] for row in self.cohort_sides}
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

    def dependency_sha_audit(self) -> list[dict[str, Any]]:
        rows = [
            ("post_cohort_003_certified_state", PARENT_DIR, EXPECTED_PARENT_SHA),
            ("remaining_scale_up_design", SCALE_DIR, EXPECTED_SCALE_SHA),
        ]
        return [
            {
                "package": name,
                "path": str(path),
                "expected_sha": expected,
                "actual_sha": package_sha(path),
                "status": "PASS" if package_sha(path) == expected else "FAIL",
            }
            for name, path, expected in rows
        ]

    def cumulative_state_verification(self) -> list[dict[str, Any]]:
        expected = {
            "decision": (
                "STARTER_DISCOVERY_COHORT_003_RECONSTRUCTION_REMEDIATION_DECISION = "
                "DISCOVERY_TO_ACQUISITION_TO_REMEDIATION_PIPELINE_REVALIDATED_CONTINUE_NEXT_COHORT"
            ),
            "certified_state": "STARTER_POST_COHORT_003_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED",
            "total_fully_qualified_hits": 1033,
            "fully_qualified_hits_0_5": 912,
            "fully_qualified_hits_1_5": 121,
            "current_starter_blocked_population": 603,
            "current_pa_blocked_population": 11,
            "current_outcome_blocked_population": 363,
            "current_bundle_blocked_population": 36,
            "qualified_but_not_matrix_constructed_hits_1_5_rows": 22,
        }
        rows = []
        for key, expected_value in expected.items():
            observed = self.parent_state.get(key)
            rows.append(
                {
                    "metric": key,
                    "observed": observed,
                    "expected": expected_value,
                    "status": "PASS" if observed == expected_value else "FAIL",
                }
            )
        rows.append(
            {
                "metric": "package_sha",
                "observed": package_sha(PARENT_DIR),
                "expected": EXPECTED_PARENT_SHA,
                "status": "PASS" if package_sha(PARENT_DIR) == EXPECTED_PARENT_SHA else "FAIL",
            }
        )
        return rows

    def confirmed_side_manifest(self) -> list[dict[str, Any]]:
        rows = []
        for idx, side in enumerate(self.cohort_sides, 1):
            rows.append(
                {
                    "cohort_id": COHORT_ID,
                    "target_order": idx,
                    "starter_game_side_key": side["starter_game_side_key"],
                    "parent_state_package": str(PARENT_DIR),
                    "parent_state_sha": EXPECTED_PARENT_SHA,
                    "current_campaign_category": side["current_campaign_category"],
                    "represented_denominator_rows": side["represented_denominator_rows"],
                    "hits_0_5_rows": side["hits_0_5_rows"],
                    "hits_1_5_rows": side["hits_1_5_rows"],
                    "rows_with_all_non_starter_prerequisites_satisfied": side["rows_with_all_non_starter_prerequisites_satisfied"],
                    "projected_starter_qualified_ceiling": side["represented_denominator_rows"],
                    "projected_newly_fully_qualified_ceiling": side["projected_newly_fully_qualified_ceiling"],
                    "downstream_pa_blockers": side["downstream_pa_blockers"],
                    "downstream_outcome_blockers": side["downstream_outcome_blockers"],
                    "downstream_bundle_blockers": side["downstream_bundle_blockers"],
                    "discovery_classification": side["discovery_classification"],
                    "discovery_target_type": side["discovery_target_type"],
                    "expected_discovery_key": side["expected_discovery_key"],
                    "expected_discovery_source": side["expected_discovery_source"],
                    "role_regime_status": side["role_regime_status"],
                    "temporal_eligibility": side["temporal_eligibility"],
                    "potential_abd_matrix_readiness_additions": side["potential_abd_matrix_readiness_additions"],
                    "estimated_later_historical_acquisition_request_count": side[
                        "estimated_later_historical_acquisition_request_count"
                    ],
                    "governance_status": "FROZEN_EXACT_SIDE",
                }
            )
        return rows

    def confirmed_row_manifest(self) -> list[dict[str, Any]]:
        rows = []
        order_by_side = {side: idx for idx, side in enumerate(self.cohort_side_keys, 1)}
        for row in sorted(self.cohort_rows, key=lambda r: (order_by_side[r["starter_game_side_key"]], row_id(r))):
            parts = split_row_id(row_id(row))
            rows.append(
                {
                    "cohort_id": COHORT_ID,
                    "target_order": order_by_side[row["starter_game_side_key"]],
                    "governed_canonical_row_id": row_id(row),
                    "starter_game_side_key": row["starter_game_side_key"],
                    **parts,
                    "player_name": row["player_name"],
                    "team": row["team"],
                    "opponent": row["opponent"],
                    "current_campaign_category": row["current_campaign_category"],
                    "current_starter_status": row["current_starter_status"],
                    "current_starter_qualified": row["current_starter_qualified"],
                    "current_full_qualification_status": row["current_full_qualification_status"],
                    "current_fully_qualified": row["current_fully_qualified"],
                    "downstream_pa_status": row["downstream_pa_status"],
                    "downstream_pa_qualified": row["downstream_pa_qualified"],
                    "downstream_outcome_status": row["downstream_outcome_status"],
                    "downstream_outcome_qualified": row["downstream_outcome_qualified"],
                    "downstream_bundle_status": row["downstream_bundle_status"],
                    "remaining_downstream_blocker": row["remaining_downstream_blocker"],
                    "authoritative_source_package": row["authoritative_source_package"],
                    "parent_state_package": str(PARENT_DIR),
                    "parent_state_sha": EXPECTED_PARENT_SHA,
                    "governance_status": "FROZEN_EXACT_ROW",
                }
            )
        return rows

    def confirmed_target_manifest(self) -> list[dict[str, Any]]:
        rows = []
        for idx, side in enumerate(self.cohort_sides, 1):
            target_date, game_id, hitter_team, opponent_team = side["starter_game_side_key"].split("|")
            rows.append(
                {
                    "cohort_id": COHORT_ID,
                    "target_order": idx,
                    "starter_game_side_key": side["starter_game_side_key"],
                    "governed_target_date": target_date,
                    "governed_target_game": game_id,
                    "hitter_team": hitter_team,
                    "opponent_team": opponent_team,
                    "discovery_target_key": side["expected_discovery_key"],
                    "discovery_target_type": side["discovery_target_type"],
                    "allowed_source_hierarchy": "official_game_boxscore_or_project_repository_preserved_game_metadata",
                    "allowed_endpoint_or_source_class": "governed_discovery_only_no_acquisition_without_separate_approval",
                    "raw_discovery_request_cap": "1",
                    "estimated_later_historical_acquisition_request_count": side[
                        "estimated_later_historical_acquisition_request_count"
                    ],
                    "date_and_temporal_boundary": "strict_prior_to_target_slate_date_for_future_acquisition",
                    "pitcher_and_target_game_binding_rule": "exact governed opponent starter for exact target game side",
                    "identity_acceptance_criteria": "exact governed side, exact game, exact opponent starter identity required",
                    "ambiguity_rejection_criteria": "fail_closed_on_identity_or_role_ambiguity",
                    "role_regime_acceptance_rule": "starter-compatible only; role regime reviewed during discovery/acquisition steps",
                    "role_regime_rejection_rule": "reject opener, bulk, follower, or incompatible role unless separately governed",
                    "duplicate_response_handling": "deduplicate by exact target identity; fail closed on conflicting source records",
                    "repeated_pitcher_handling": "repeated pitcher overlap is reportable but not a conflict without identity collision",
                    "raw_response_preservation": "required in any later approved discovery/acquisition execution",
                    "parser_provenance_contract": "raw source and parsed ledger required in any later approved step",
                    "bounded_retry_limit": "bounded by future explicit approval; no broad crawling",
                    "search_boundary": "no unrelated player, pitcher, game, or date search",
                    "fail_closed_taxonomy": "identity_ambiguous|role_incompatible|source_missing|duplicate_conflict|temporal_violation",
                    "deterministic_offline_replay": "required before any future acquisition or remediation step",
                    "discovery_to_acquisition_conversion_rule": "successful discovery may define acquisition requests only after separate approval",
                    "approval_boundary": "discovery approval does not authorize acquisition; acquisition approval does not authorize remediation",
                }
            )
        return rows

    def overlap_audit(self) -> list[dict[str, Any]]:
        completed = {
            "four_side_history_complete_remediation": FOUR_SIDE_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv",
            "hc_local_cohort_001": HC_LOCAL_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv",
            **{
                name: path / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
                for name, path in COHORT_DIRS.items()
            },
        }
        rows = []
        for name, path in completed.items():
            data = read_csv(path) if path.exists() else []
            other_row_ids = {row_id(row) for row in data}
            other_sides = {side_from_movement(row) for row in data}
            row_overlap = self.cohort_row_ids & other_row_ids
            side_overlap = self.cohort_side_set & other_sides
            rows.append(
                {
                    "compared_population": name,
                    "source_path": str(path),
                    "row_overlap": len(row_overlap),
                    "side_overlap": len(side_overlap),
                    "pitcher_overlap": "unknown_until_discovery",
                    "discovery_target_overlap": "not_applicable_completed_overlay",
                    "status": "PASS" if not row_overlap and not side_overlap else "FAIL",
                    "overlap_row_ids": "|".join(sorted(row_overlap)),
                    "overlap_side_ids": "|".join(sorted(side_overlap)),
                }
            )

        category_sets = {
            "local_parent_prescreen_fail_closed_population": {
                row["starter_game_side_key"]
                for row in self.sides_96
                if row["current_campaign_category"] == "LOCAL_PARENT_PRESCREEN_FAIL_CLOSED"
            },
            "identity_role_review_holdout_population": {row["starter_game_side_key"] for row in self.role_holdouts},
            "ordinary_downstream_limited_population": {row["starter_game_side_key"] for row in self.downstream_limited},
        }
        for name, sides in category_sets.items():
            side_overlap = self.cohort_side_set & sides
            rows.append(
                {
                    "compared_population": name,
                    "source_path": "scale_up_design_ledgers",
                    "row_overlap": "not_evaluated_side_population",
                    "side_overlap": len(side_overlap),
                    "pitcher_overlap": "unknown_until_discovery",
                    "discovery_target_overlap": "unknown_until_discovery",
                    "status": "PASS" if not side_overlap else "FAIL",
                    "overlap_row_ids": "",
                    "overlap_side_ids": "|".join(sorted(side_overlap)),
                }
            )

        for name, path in TARGET_MANIFEST_DIRS.items():
            manifest_path = path / f"exact_governed_target_manifest_{RUN_DATE}.csv"
            data = read_csv(manifest_path) if manifest_path.exists() else []
            targets = {row.get("discovery_target_key", "") for row in data}
            overlap = self.cohort_targets & targets
            rows.append(
                {
                    "compared_population": f"{name}_target_manifest",
                    "source_path": str(manifest_path),
                    "row_overlap": 0,
                    "side_overlap": 0,
                    "pitcher_overlap": "unknown_until_discovery",
                    "discovery_target_overlap": len(overlap),
                    "status": "PASS" if not overlap else "FAIL",
                    "overlap_row_ids": "",
                    "overlap_side_ids": "",
                }
            )

        duplicate_rows = [
            row for row, count in Counter(row_id(r) for r in self.cohort_rows).items() if count > 1
        ]
        duplicate_sides = [side for side, count in Counter(self.cohort_side_keys).items() if count > 1]
        rows.append(
            {
                "compared_population": "within_cohort_duplicate_governed_rows",
                "source_path": str(SCALE_803),
                "row_overlap": len(duplicate_rows),
                "side_overlap": 0,
                "pitcher_overlap": "not_applicable",
                "discovery_target_overlap": "not_applicable",
                "status": "PASS" if not duplicate_rows else "FAIL",
                "overlap_row_ids": "|".join(sorted(duplicate_rows)),
                "overlap_side_ids": "",
            }
        )
        rows.append(
            {
                "compared_population": "within_cohort_duplicate_governed_sides",
                "source_path": str(SCALE_PLAN),
                "row_overlap": 0,
                "side_overlap": len(duplicate_sides),
                "pitcher_overlap": "not_applicable",
                "discovery_target_overlap": "not_applicable",
                "status": "PASS" if not duplicate_sides else "FAIL",
                "overlap_row_ids": "",
                "overlap_side_ids": "|".join(sorted(duplicate_sides)),
            }
        )
        return rows

    def remaining_population_reconciliation(self) -> list[dict[str, Any]]:
        pre_c3_side_counts = Counter(row["current_campaign_category"] for row in self.sides_96)
        pre_c3_row_counts = Counter(row["current_campaign_category"] for row in self.rows_803)
        c3_side_keys = set()
        c3_rows = []
        c3_path = COHORT_DIRS["discovery_cohort_003"] / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
        if c3_path.exists():
            c3_rows = read_csv(c3_path)
            c3_side_keys = {side_from_movement(row) for row in c3_rows}
        remaining_ordinary_sides = [
            row
            for row in self.remaining_sides
            if row["starter_game_side_key"] not in c3_side_keys
            and row["current_campaign_category"] == "DISCOVERY_SCALE_UP_CANDIDATE"
        ]
        accounted_side_keys = {
            row["starter_game_side_key"]
            for row in self.sides_96
            if row["current_campaign_category"] != "DISCOVERY_SCALE_UP_CANDIDATE"
        } | c3_side_keys | {row["starter_game_side_key"] for row in remaining_ordinary_sides}
        pre_c3_remediated_sides = (
            pre_c3_side_counts["STARTER_REMEDIATED_FULLY_QUALIFIED"]
            + pre_c3_side_counts["STARTER_REMEDIATED_DOWNSTREAM_BLOCKED"]
        )
        pre_c3_remediated_rows = (
            pre_c3_row_counts["STARTER_REMEDIATED_FULLY_QUALIFIED"]
            + pre_c3_row_counts["STARTER_REMEDIATED_DOWNSTREAM_BLOCKED"]
        )
        rows = [
            {
                "population": "original_campaign_total",
                "side_count": len(self.sides_96),
                "row_count": len(self.rows_803),
                "projected_fully_qualified_ceiling": sum(int_value(r.get("projected_fully_qualified_ceiling")) for r in self.sides_96),
                "notes": "Authoritative 96-side / 803-row campaign.",
            },
            {
                "population": "already_starter_remediated_before_cohort_003_all_statuses",
                "side_count": pre_c3_remediated_sides,
                "row_count": pre_c3_remediated_rows,
                "projected_fully_qualified_ceiling": "",
                "notes": "Completed overlays reflected in the scale-up design before COHORT_003, including fully qualified and downstream-blocked remediated rows.",
            },
            {
                "population": "cohort_003_newly_remediated",
                "side_count": len(c3_side_keys),
                "row_count": len(c3_rows),
                "projected_fully_qualified_ceiling": len(c3_rows),
                "notes": "Certified movement added by post-COHORT_003 parent state.",
            },
            {
                "population": "local_parent_fail_closed",
                "side_count": pre_c3_side_counts["LOCAL_PARENT_PRESCREEN_FAIL_CLOSED"],
                "row_count": pre_c3_row_counts["LOCAL_PARENT_PRESCREEN_FAIL_CLOSED"],
                "projected_fully_qualified_ceiling": "",
                "notes": "Fail-closed local-parent pre-screen population; not eligible for ordinary discovery cohort substitution.",
            },
            {
                "population": "ordinary_downstream_limited",
                "side_count": pre_c3_side_counts["ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED"],
                "row_count": pre_c3_row_counts["ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED"],
                "projected_fully_qualified_ceiling": sum(int_value(r.get("projected_fully_qualified_ceiling")) for r in self.downstream_limited),
                "notes": "Held out because downstream blockers remain before Starter action is worth governing.",
            },
            {
                "population": "identity_role_review_holdouts",
                "side_count": len(self.role_holdouts),
                "row_count": sum(int_value(r["represented_denominator_rows"]) for r in self.role_holdouts),
                "projected_fully_qualified_ceiling": sum(int_value(r["projected_newly_fully_qualified_ceiling"]) for r in self.role_holdouts),
                "notes": "Separately classified role/identity review population.",
            },
            {
                "population": "remaining_ordinary_discovery_candidates_after_cohort_003",
                "side_count": len(remaining_ordinary_sides),
                "row_count": sum(int_value(r["represented_denominator_rows"]) for r in remaining_ordinary_sides),
                "projected_fully_qualified_ceiling": sum(
                    int_value(r["projected_newly_fully_qualified_ceiling"]) for r in remaining_ordinary_sides
                ),
                "notes": "Scale-up ordinary candidates minus certified COHORT_003 sides.",
            },
            {
                "population": "cohort_004_existing_plan_subset",
                "side_count": len(self.cohort_side_keys),
                "row_count": len(self.cohort_rows),
                "projected_fully_qualified_ceiling": sum(
                    int_value(r["projected_newly_fully_qualified_ceiling"]) for r in self.cohort_sides
                ),
                "notes": "Frozen unchanged if validation passes.",
            },
            {
                "population": "accounted_original_sides_once_check",
                "side_count": len(accounted_side_keys),
                "row_count": pre_c3_remediated_rows
                + len(c3_rows)
                + pre_c3_row_counts["LOCAL_PARENT_PRESCREEN_FAIL_CLOSED"]
                + pre_c3_row_counts["ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED"]
                + pre_c3_row_counts["DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED"]
                + sum(int_value(r["represented_denominator_rows"]) for r in remaining_ordinary_sides),
                "projected_fully_qualified_ceiling": "",
                "notes": "Must equal 96 original sides and 803 original rows after category accounting.",
            },
            {
                "population": "current_global_starter_blocked_rows_from_parent_state",
                "side_count": "not_side_scoped",
                "row_count": self.parent_state["current_starter_blocked_population"],
                "projected_fully_qualified_ceiling": "",
                "notes": "Certified cumulative global blocker count after COHORT_003.",
            },
        ]
        return rows

    def unchanged_preservation_analysis(self) -> list[dict[str, Any]]:
        return [
            {
                "analysis_item": "cohort_preservation",
                "status": "UNCHANGED",
                "evidence": "Existing DISCOVERY_COHORT_004 side_keys reproduced exactly from full_remaining_cohort_plan_2026-07-15.csv.",
                "action": "No redesign or substitution.",
            },
            {
                "analysis_item": "invalid_side_or_row_detection",
                "status": "NONE_FOUND",
                "evidence": "All eight sides remain DISCOVERY_SCALE_UP_CANDIDATE and all 73 governed rows remain current_starter_qualified=false.",
                "action": "Freeze unchanged.",
            },
            {
                "analysis_item": "overlap_conflicts",
                "status": "NONE_FOUND",
                "evidence": "Overlap audit reports zero row conflicts, zero side conflicts, zero duplicate governed rows, and zero duplicate governed sides.",
                "action": "Freeze unchanged.",
            },
            {
                "analysis_item": "projection_update",
                "status": "UPDATED_TO_POST_COHORT_003_PARENT",
                "evidence": "Projected cumulative totals start from certified post-COHORT_003 state: 1,033 fully qualified Hits and 603 Starter-blocked rows.",
                "action": "Use post-COHORT_003 parent package and SHA.",
            },
        ]

    def cohort_status_reconciliation(self) -> list[dict[str, Any]]:
        side_manifest = self.confirmed_side_manifest()
        rows = []
        for side in side_manifest:
            key = side["starter_game_side_key"]
            side_rows = [row for row in self.cohort_rows if row["starter_game_side_key"] == key]
            rows.append(
                {
                    "starter_game_side_key": key,
                    "remains_starter_blocked": str(all(r["current_starter_qualified"] == "false" for r in side_rows)).lower(),
                    "not_already_remediated": str(side["current_campaign_category"] == "DISCOVERY_SCALE_UP_CANDIDATE").lower(),
                    "remains_discovery_eligible": str(side["current_campaign_category"] == "DISCOVERY_SCALE_UP_CANDIDATE").lower(),
                    "remains_temporally_eligible": side["temporal_eligibility"],
                    "remains_ordinary_under_frozen_role_rules": str(side["role_regime_status"] == "unknown_until_source_review").lower(),
                    "projected_starter_ceiling": side["projected_starter_qualified_ceiling"],
                    "projected_full_ceiling": side["projected_newly_fully_qualified_ceiling"],
                    "status": "PASS"
                    if all(r["current_starter_qualified"] == "false" for r in side_rows)
                    and side["current_campaign_category"] == "DISCOVERY_SCALE_UP_CANDIDATE"
                    else "FAIL",
                }
            )
        return rows

    def governance_contract(self) -> list[dict[str, Any]]:
        return [
            {"contract_item": "parent_state_reference", "frozen_value": str(PARENT_DIR), "notes": "Parent is certified cumulative post-COHORT_003 state."},
            {"contract_item": "parent_state_sha", "frozen_value": EXPECTED_PARENT_SHA, "notes": "Must verify before any future discovery execution."},
            {"contract_item": "side_manifest", "frozen_value": "exact 8 sides preserved from existing DISCOVERY_COHORT_004 scale-up plan", "notes": "No redesign performed."},
            {"contract_item": "row_manifest", "frozen_value": "exact 73 governed denominator rows", "notes": "No population expansion or replacement."},
            {"contract_item": "discovery_target_cap", "frozen_value": "8", "notes": "One target per governed side."},
            {"contract_item": "estimated_later_acquisition_volume", "frozen_value": "240", "notes": "Acquisition requires separate explicit approval."},
            {"contract_item": "date_and_temporal_boundaries", "frozen_value": "target dates 2026-07-02 through 2026-07-08; strict prior for future source records", "notes": "No same-game postgame facts for strict-prior domains."},
            {"contract_item": "pitcher_target_game_binding", "frozen_value": "exact governed opponent starter for exact side/game only", "notes": "No unrelated player, pitcher, game, or date search."},
            {"contract_item": "identity_acceptance", "frozen_value": "exact governed side, exact game, exact opponent starter identity required", "notes": "Fail closed on identity ambiguity."},
            {"contract_item": "role_regime_rules", "frozen_value": "starter-compatible only; reject incompatible opener/bulk/follower role unless separately governed", "notes": "Unknown source regimes stay fail-closed until reviewed."},
            {"contract_item": "duplicate_response_handling", "frozen_value": "deduplicate exact target identity; fail closed on conflicting records", "notes": "No source substitution."},
            {"contract_item": "raw_response_preservation", "frozen_value": "required in any later approved discovery/acquisition execution", "notes": "Governance only here."},
            {"contract_item": "parser_and_provenance", "frozen_value": "raw source, parsed ledger, validation report, and SHA references required", "notes": "Deterministic offline replay is required."},
            {"contract_item": "bounded_retry_limits", "frozen_value": "bounded by future explicit approval", "notes": "No broad crawling."},
            {"contract_item": "discovery_to_acquisition_conversion", "frozen_value": "discovery may define acquisition requests only after separate explicit approval", "notes": "Discovery approval is not acquisition approval."},
            {"contract_item": "acquisition_to_remediation_boundary", "frozen_value": "acquisition approval does not authorize reconstruction/remediation", "notes": "Separate freeze and approval required."},
        ]

    def approval_boundary(self) -> list[dict[str, Any]]:
        return [
            {"approval_item": "authorized_now", "value": "freeze cumulative-state-governed DISCOVERY_COHORT_004 only"},
            {"approval_item": "next_separate_approval_required", "value": "bounded discovery execution for exact 8 COHORT_004 sides only"},
            {
                "approval_item": "not_authorized",
                "value": (
                    "discovery execution|acquisition|reconstruction|remediation|qualification propagation|"
                    "matrix construction|model/scoring|DB/API writes|OddsAPI|uploads|LaunchAgent|production changes"
                ),
            },
        ]

    def projections(self) -> dict[str, Any]:
        full_rows = [
            row
            for row in self.cohort_rows
            if boolish(row["downstream_pa_qualified"])
            and boolish(row["downstream_outcome_qualified"])
            and row["downstream_bundle_status"] == "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING"
        ]
        hits_05_full = sum(1 for row in full_rows if row["line"] == "0.5")
        hits_15_full = sum(1 for row in full_rows if row["line"] == "1.5")
        pa_blockers = sum(1 for row in self.cohort_rows if not boolish(row["downstream_pa_qualified"]))
        outcome_blockers = sum(1 for row in self.cohort_rows if not boolish(row["downstream_outcome_qualified"]))
        bundle_blockers = sum(1 for row in self.cohort_rows if row["downstream_bundle_status"] != "HITS_STARTER_BLOCKED_DIRECT_SOURCE_MISSING")
        return {
            "governed_side_count": len(self.cohort_side_keys),
            "governed_row_count": len(self.cohort_rows),
            "discovery_target_cap": len(self.cohort_side_keys),
            "projected_starter_qualified_ceiling": len(self.cohort_rows),
            "projected_newly_fully_qualified_ceiling": len(full_rows),
            "projected_hits_0_5_additions": hits_05_full,
            "projected_hits_1_5_additions": hits_15_full,
            "downstream_limited_rows": pa_blockers + outcome_blockers + bundle_blockers,
            "projected_pa_blockers": pa_blockers,
            "projected_outcome_blockers": outcome_blockers,
            "projected_bundle_blockers": bundle_blockers,
            "potential_abd_matrix_readiness_additions": sum(
                int_value(row["potential_abd_matrix_readiness_additions"]) for row in self.cohort_sides
            ),
            "estimated_later_acquisition_requests": sum(
                int_value(row["estimated_later_historical_acquisition_request_count"]) for row in self.cohort_sides
            ),
            "projected_cumulative_post_cohort_004_total_fully_qualified_hits": self.parent_state[
                "total_fully_qualified_hits"
            ]
            + len(full_rows),
            "projected_cumulative_post_cohort_004_hits_0_5": self.parent_state["fully_qualified_hits_0_5"]
            + hits_05_full,
            "projected_cumulative_post_cohort_004_hits_1_5": self.parent_state["fully_qualified_hits_1_5"]
            + hits_15_full,
            "projected_cumulative_post_cohort_004_starter_blocked": self.parent_state[
                "current_starter_blocked_population"
            ]
            - len(self.cohort_rows),
            "projected_cumulative_post_cohort_004_pa_blocked": self.parent_state["current_pa_blocked_population"]
            + pa_blockers,
            "projected_cumulative_post_cohort_004_outcome_blocked": self.parent_state[
                "current_outcome_blocked_population"
            ]
            + outcome_blockers,
            "projected_cumulative_post_cohort_004_bundle_blocked": self.parent_state[
                "current_bundle_blocked_population"
            ]
            + bundle_blockers,
            "projected_cumulative_post_cohort_004_hits_1_5_queue": self.parent_state[
                "qualified_but_not_matrix_constructed_hits_1_5_rows"
            ]
            + hits_15_full,
        }

    def frozen_projection_rows(self) -> list[dict[str, Any]]:
        return [{"metric": key, "value": value, "notes": "Projection ceiling; not realized outcome."} for key, value in self.projections().items()]

    def validate(self, overlap: list[dict[str, Any]], side_status: list[dict[str, Any]]) -> list[dict[str, Any]]:
        side_manifest = self.confirmed_side_manifest()
        row_manifest = self.confirmed_row_manifest()
        target_manifest = self.confirmed_target_manifest()
        projections = self.projections()
        expected_plan = {
            "exact_cohort_004_side_count": int_value(self.cohort_plan["side_count"]),
            "exact_cohort_004_row_count": int_value(self.cohort_plan["represented_row_count"]),
            "exact_discovery_target_cap": int_value(self.cohort_plan["discovery_target_count"]),
            "projected_starter_qualified_ceiling": int_value(self.cohort_plan["represented_row_count"]),
            "projected_newly_fully_qualified_ceiling": int_value(self.cohort_plan["projected_newly_fully_qualified_ceiling"]),
            "projected_hits_0_5_additions": int_value(self.cohort_plan["hits_0_5_row_count"])
            - int_value(self.cohort_plan["downstream_pa_blockers"]),
            "projected_hits_1_5_additions": int_value(self.cohort_plan["hits_1_5_row_count"]),
            "projected_abd_additions": int_value(self.cohort_plan["potential_abd_matrix_readiness_additions"]),
            "estimated_later_acquisition_volume": int_value(self.cohort_plan["estimated_historical_acquisition_request_count"]),
        }
        checks = [
            ("parent_post_cohort_003_sha_verification", package_sha(PARENT_DIR), EXPECTED_PARENT_SHA),
            ("scale_up_package_sha_verification", package_sha(SCALE_DIR), EXPECTED_SCALE_SHA),
            ("exact_existing_cohort_004_side_count", len(side_manifest), expected_plan["exact_cohort_004_side_count"]),
            ("exact_existing_cohort_004_row_count", len(row_manifest), expected_plan["exact_cohort_004_row_count"]),
            ("exact_discovery_target_cap", len(target_manifest), expected_plan["exact_discovery_target_cap"]),
            ("projected_starter_qualified_ceiling", projections["projected_starter_qualified_ceiling"], expected_plan["projected_starter_qualified_ceiling"]),
            ("projected_newly_fully_qualified_ceiling", projections["projected_newly_fully_qualified_ceiling"], expected_plan["projected_newly_fully_qualified_ceiling"]),
            ("projected_hits_0_5_additions", projections["projected_hits_0_5_additions"], expected_plan["projected_hits_0_5_additions"]),
            ("projected_hits_1_5_additions", projections["projected_hits_1_5_additions"], expected_plan["projected_hits_1_5_additions"]),
            ("projected_abd_additions", projections["potential_abd_matrix_readiness_additions"], expected_plan["projected_abd_additions"]),
            ("estimated_later_acquisition_volume", projections["estimated_later_acquisition_requests"], expected_plan["estimated_later_acquisition_volume"]),
            ("zero_duplicate_governed_rows", len({r["governed_canonical_row_id"] for r in row_manifest}), len(row_manifest)),
            ("zero_duplicate_governed_sides", len({r["starter_game_side_key"] for r in side_manifest}), len(side_manifest)),
            ("all_sides_still_discovery_eligible", sum(r["status"] == "PASS" for r in side_status), len(side_status)),
            ("exact_overlap_audit", sum(r["status"] == "PASS" for r in overlap), len(overlap)),
            (
                "exact_current_remaining_population_side_reproduction",
                next(r for r in self.remaining_population_reconciliation() if r["population"] == "accounted_original_sides_once_check")[
                    "side_count"
                ],
                96,
            ),
            (
                "exact_current_remaining_population_row_reproduction",
                next(r for r in self.remaining_population_reconciliation() if r["population"] == "accounted_original_sides_once_check")[
                    "row_count"
                ],
                803,
            ),
            ("no_population_expansion", len(row_manifest), int_value(self.cohort_plan["represented_row_count"])),
            (
                "no_opposite_side_creation",
                "|".join(sorted(self.cohort_side_keys)),
                "|".join(sorted(row["starter_game_side_key"] for row in side_manifest)),
            ),
            ("post_cohort_004_total_fully_qualified_projection", projections["projected_cumulative_post_cohort_004_total_fully_qualified_hits"], 1102),
            ("post_cohort_004_starter_blocked_projection", projections["projected_cumulative_post_cohort_004_starter_blocked"], 530),
            ("post_cohort_004_pa_blocked_projection", projections["projected_cumulative_post_cohort_004_pa_blocked"], 15),
            ("post_cohort_004_hits_1_5_queue_projection", projections["projected_cumulative_post_cohort_004_hits_1_5_queue"], 28),
        ]
        rows = [
            {"validation": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected}
            for name, observed, expected in checks
        ]
        rows.extend(
            [
                {"validation": item, "status": "PASS", "observed": "not_performed", "expected": "not_performed"}
                for item in [
                    "discovery_execution",
                    "acquisition",
                    "reconstruction",
                    "remediation",
                    "qualification_propagation",
                    "formula_changes",
                    "matrix_construction",
                    "model_signal_scoring",
                    "database_api_writes",
                    "oddsapi_calls",
                    "uploads_launchagent_production_change",
                ]
            ]
        )
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        rows.append(
            {
                "validation": "existing_abd_matrices_byte_identical",
                "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL",
                "observed": json.dumps(matrix_after, sort_keys=True),
                "expected": json.dumps(self.matrix_hash_before, sort_keys=True),
            }
        )
        rows.append(
            {
                "validation": "static_guard",
                "status": "PASS" if all(r["status"] == "PASS" for r in static_guard()) else "FAIL",
                "observed": "see_static_guard",
                "expected": "all_pass",
            }
        )
        return rows

    def run(self) -> dict[str, Any]:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        side_manifest = self.confirmed_side_manifest()
        row_manifest = self.confirmed_row_manifest()
        target_manifest = self.confirmed_target_manifest()
        overlap = self.overlap_audit()
        remaining = self.remaining_population_reconciliation()
        side_status = self.cohort_status_reconciliation()
        validation = self.validate(overlap, side_status)
        if any(row["status"] != "PASS" for row in validation):
            write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
            raise RuntimeError("COHORT_004 cumulative governance validation failed")

        projections = self.projections()
        payload = {
            "decision": DECISION,
            "status": STATUS,
            "generated_at": GENERATED_AT,
            "parent_cumulative_state_package": str(PARENT_DIR),
            "parent_cumulative_state_sha": EXPECTED_PARENT_SHA,
            "scale_up_design_package_sha": EXPECTED_SCALE_SHA,
            "cohort_redesigned": False,
            "discovery_executed": False,
            "acquisition_executed": False,
            "reconstruction_or_remediation_executed": False,
            "next_separate_approval_required": "bounded discovery execution for exact 8 DISCOVERY_COHORT_004 sides only",
            **projections,
        }

        write_csv(OUT_DIR / f"cumulative_state_verification_{RUN_DATE}.csv", self.cumulative_state_verification())
        write_csv(OUT_DIR / f"dependency_sha_audit_{RUN_DATE}.csv", self.dependency_sha_audit())
        write_csv(OUT_DIR / f"overlap_audit_{RUN_DATE}.csv", overlap)
        write_csv(OUT_DIR / f"remaining_population_reconciliation_{RUN_DATE}.csv", remaining)
        write_csv(OUT_DIR / f"unchanged_preservation_analysis_{RUN_DATE}.csv", self.unchanged_preservation_analysis())
        write_csv(OUT_DIR / f"cohort_status_reconciliation_{RUN_DATE}.csv", side_status)
        write_csv(OUT_DIR / f"confirmed_side_manifest_{RUN_DATE}.csv", side_manifest)
        write_csv(OUT_DIR / f"confirmed_row_manifest_{RUN_DATE}.csv", row_manifest)
        write_csv(OUT_DIR / f"confirmed_discovery_target_manifest_{RUN_DATE}.csv", target_manifest)
        write_csv(OUT_DIR / f"frozen_projection_summary_{RUN_DATE}.csv", self.frozen_projection_rows())
        write_csv(OUT_DIR / f"cumulative_state_governance_contract_{RUN_DATE}.csv", self.governance_contract())
        write_csv(OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.csv", self.approval_boundary())
        write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
        write_json(OUT_DIR / f"machine_readable_cumulative_governance_{RUN_DATE}.json", payload)
        write_md(
            OUT_DIR / f"executive_summary_{RUN_DATE}.md",
            f"""
# DISCOVERY_COHORT_004 Cumulative-State Governance — {RUN_DATE}

Decision: `{DECISION}`

Status: `{STATUS}`

The existing DISCOVERY_COHORT_004 plan was reconciled against the certified
post-COHORT_003 cumulative campaign state and frozen unchanged. The updated
parent-state reference is `{PARENT_DIR}` with SHA `{EXPECTED_PARENT_SHA}`.

- Exact governed sides: `{payload['governed_side_count']}`
- Exact governed rows: `{payload['governed_row_count']}`
- Discovery target cap: `{payload['discovery_target_cap']}`
- Projected Starter-qualified ceiling: `{payload['projected_starter_qualified_ceiling']}`
- Projected newly fully qualified ceiling: `{payload['projected_newly_fully_qualified_ceiling']}`
- Projected Hits 0.5 additions: `{payload['projected_hits_0_5_additions']}`
- Projected Hits 1.5 additions: `{payload['projected_hits_1_5_additions']}`
- Projected A/B/D additions: `{payload['potential_abd_matrix_readiness_additions']}`
- Estimated later acquisition requests: `{payload['estimated_later_acquisition_requests']}`
- Projected cumulative fully qualified Hits after successful certification: `{payload['projected_cumulative_post_cohort_004_total_fully_qualified_hits']}`
- Projected cumulative Starter-blocked rows after successful certification: `{payload['projected_cumulative_post_cohort_004_starter_blocked']}`

The overlap audit found zero row conflicts, zero side conflicts, zero duplicate
governed rows, and zero duplicate governed sides. Pitcher overlap remains
unknown until a separately approved discovery step identifies starters.

No discovery, acquisition, reconstruction, remediation, qualification
propagation, matrix construction, modeling, scoring, database/API write,
OddsAPI call, upload, LaunchAgent edit, or production change occurred.

Next separate approval required: bounded discovery execution for the exact 8
DISCOVERY_COHORT_004 sides only.
""",
        )
        self.parse_and_hash()
        return {**payload, "output_dir": str(OUT_DIR), "package_sha256_manifest_hash": package_sha(OUT_DIR)}

    def parse_and_hash(self) -> None:
        parse_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if not path.is_file() or path.name in {
                f"sha256_manifest_{RUN_DATE}.csv",
                f"parse_validation_{RUN_DATE}.csv",
            }:
                continue
            try:
                if path.suffix == ".csv":
                    read_csv(path)
                    kind = "csv"
                elif path.suffix == ".json":
                    json.loads(path.read_text(encoding="utf-8"))
                    kind = "json"
                elif path.suffix == ".md":
                    kind = "markdown"
                    if not path.read_text(encoding="utf-8").lstrip().startswith("#"):
                        raise ValueError("markdown missing heading")
                else:
                    continue
                status = "PASS"
                notes = ""
            except Exception as exc:
                kind = path.suffix.lstrip(".")
                status = "FAIL"
                notes = str(exc)
            parse_rows.append({"path": str(path), "artifact_type": kind, "parse_status": status, "notes": notes})
        write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)
        sha_rows = []
        for path in sorted(OUT_DIR.rglob("*")):
            if path.is_file() and path.name != f"sha256_manifest_{RUN_DATE}.csv":
                sha_rows.append({"path": str(path), "sha256": sha256_path(path), "bytes": path.stat().st_size})
        write_csv(OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv", sha_rows)


def main() -> int:
    result = Cohort004CumulativeGovernance().run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
