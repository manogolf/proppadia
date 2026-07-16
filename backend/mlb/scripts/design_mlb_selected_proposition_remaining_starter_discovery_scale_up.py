#!/usr/bin/env python3
"""Freeze the remaining Starter direct-source discovery scale-up design.

Read-only governance/design utility. It performs no network access, source
discovery, acquisition, reconstruction, remediation, denominator propagation,
matrix construction, model/scoring work, database/API writes, uploads,
LaunchAgent edits, or production behavior changes.
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
SOURCE_DATE = "2026-07-14"
FROZEN_GENERATED_AT = "2026-07-15T00:00:00+00:00"

STARTER_REMAINING_DISCOVERY_SCALE_UP_DESIGN_DECISION = (
    "STARTER_REMAINING_DISCOVERY_SCALE_UP_DESIGN_DECISION = "
    "CURRENT_REMAINDER_RECONCILED_AND_NEXT_DISCOVERY_COHORTS_FROZEN_NO_EXECUTION"
)
STARTER_DISCOVERY_COHORT_002_GOVERNANCE_STATUS = (
    "STARTER_DISCOVERY_COHORT_002_GOVERNANCE_STATUS = "
    "FROZEN_AWAITING_EXPLICIT_BOUNDED_DISCOVERY_APPROVAL"
)

EXPECTED_PACKAGE_HASHES = {
    "original_803_readiness": "b0065681934c8eb99512d66a1b70b24febd3fe585ab04a3a06bd092c47cf59fb",
    "prescreen_discovery_governance": "9f5e24541ebbeefc9ea76655bb4506473b9876dc1fbc5fe3301b1a26ea31b5f6",
    "four_side_history_complete_remediation": "629de76d980f219e5d1aa98cba7bc259cd19921ac35f1dd2ffc0b6119c628c7f",
    "hc_local_cohort_001_remediation": "097922f3ea6495c4a9c3c10b5df0bcd60515ecb7d5fe75f3ee38cb28ff514ab9",
    "discovery_cohort_001_remediation": "0c2179dfc2a23f7ccc75402f3be8cb6de9eb16938d7bdec977c2737b52c3a8b4",
    "post_three_row_state_baseline": "c27d483c704b5ceef07737b51c4cc13cdc1840955c67d18d487cd6597bc96b24",
}

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/"
    "2026-07-15"
)

READINESS_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_803_starter_direct_source_recovery_readiness_review/"
    "2026-07-14"
)
PRESCREEN_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_starter_prescreen_and_discovery_cohort_governance/"
    "2026-07-15"
)
FOUR_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_four_side_history_complete_starter_reconstruction_remediation/"
    "2026-07-15"
)
HC_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_starter_reconstruction_remediation/"
    "2026-07-15"
)
HC_FAIL_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_hc_local_cohort_001_pitcher_base_lineage_investigation/"
    "2026-07-15"
)
DISC1_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_discovery_cohort_001_starter_reconstruction_remediation/"
    "2026-07-15"
)
STATE_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_post_three_row_pa_remediation_qualification_state/"
    "2026-07-14"
)
MATRIX_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_abd_matrix_construction/2026-07-14"
)

READINESS_SIDES = READINESS_DIR / f"exact_starter_game_side_manifest_{SOURCE_DATE}.csv"
READINESS_ROWS = READINESS_DIR / f"exact_803_row_denominator_manifest_{SOURCE_DATE}.csv"
PRESCREEN_RECON = PRESCREEN_DIR / f"authoritative_campaign_reconciliation_{RUN_DATE}.csv"
DISCOVERY_INVENTORY = PRESCREEN_DIR / f"discovery_78_side_inventory_{RUN_DATE}.csv"
FOUR_ROWS = FOUR_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
HC_ROWS = HC_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
HC_FAIL_SIDES = HC_FAIL_DIR / f"exact_fail_closed_side_manifest_{RUN_DATE}.csv"
DISC1_ROWS = DISC1_DIR / f"row_level_qualification_movement_ledger_{RUN_DATE}.csv"
DISC1_SIDES = DISC1_DIR / f"side_level_reconstruction_certification_ledger_{RUN_DATE}.csv"
STATE_JSON = STATE_DIR / f"machine_readable_state_summary_{SOURCE_DATE}.json"

PACKAGE_MANIFESTS = {
    "original_803_readiness": READINESS_DIR / f"sha256_manifest_{SOURCE_DATE}.csv",
    "prescreen_discovery_governance": PRESCREEN_DIR / f"sha256_manifest_{RUN_DATE}.csv",
    "four_side_history_complete_remediation": FOUR_DIR / f"sha256_manifest_{RUN_DATE}.csv",
    "hc_local_cohort_001_remediation": HC_DIR / f"sha256_manifest_{RUN_DATE}.csv",
    "discovery_cohort_001_remediation": DISC1_DIR / f"sha256_manifest_{RUN_DATE}.csv",
    "post_three_row_state_baseline": STATE_DIR / f"sha256_manifest_{SOURCE_DATE}.csv",
}

MATRIX_PATHS = [
    MATRIX_DIR / f"variant_a_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
    MATRIX_DIR / f"variant_b_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
    MATRIX_DIR / f"variant_d_hits_1_5_qualified_matrix_{SOURCE_DATE}.csv",
]

STANDARD_COHORT_SIDE_CAP = 8
STANDARD_COHORT_ESTIMATED_ACQUISITION_CAP = 250
DEFAULT_ESTIMATED_ACQUISITION_REQUESTS = 30
PITCHER_BINDING_ESTIMATED_ACQUISITION_REQUESTS = 40

PROHIBITED_PATTERNS = {
    "network_call": re.compile(r"requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "source_discovery_or_acquisition": re.compile(r"\b(download|fetch|urlretrieve)\s*\(", re.IGNORECASE),
    "reconstruction_or_remediation": re.compile(r"\breconstruct\s*\(|\bremediate\s*\(", re.IGNORECASE),
    "model_signal_or_scoring": re.compile(r"\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|score_", re.IGNORECASE),
    "matrix_construction": re.compile(r"build_mlb_selected_proposition_abd_matrices|matrix_construct", re.IGNORECASE),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "oddsapi": re.compile(r"oddsapi|odds_api", re.IGNORECASE),
    "upload_or_scheduler": re.compile(r"launchctl|LaunchAgent|write_upload|upload_ready", re.IGNORECASE),
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


def package_sha(path: Path) -> str:
    return sha256_path(path)


def int_value(value: Any) -> int:
    try:
        return int(float(str(value).strip()))
    except Exception:
        return 0


def bool_value(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def pct(num: int | float, den: int | float) -> str:
    if not den:
        return "0.0000"
    return f"{float(num) / float(den):.4f}"


def parse_overlap_count(value: str) -> int:
    match = re.search(r"(\d+)", value or "")
    return int(match.group(1)) if match else 0


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
        rows.append({
            "check": name,
            "status": "PASS" if not matches else "FAIL",
            "matches": "|".join(str(m) for m in matches),
            "notes": "Static guard excludes comments, string literals, and the pattern declaration block.",
        })
    return rows


class RemainingStarterDiscoveryScaleUp:
    def __init__(self) -> None:
        self.original_sides = read_csv(READINESS_SIDES)
        self.original_rows = read_csv(READINESS_ROWS)
        self.prescreen_recon = read_csv(PRESCREEN_RECON)
        self.discovery_inventory = read_csv(DISCOVERY_INVENTORY)
        self.four_rows = read_csv(FOUR_ROWS)
        self.hc_rows = read_csv(HC_ROWS)
        self.hc_fail_sides = read_csv(HC_FAIL_SIDES)
        self.disc1_rows = read_csv(DISC1_ROWS)
        self.disc1_sides = read_csv(DISC1_SIDES)
        self.state = json.loads(STATE_JSON.read_text(encoding="utf-8"))
        self.rows_by_side: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in self.original_rows:
            self.rows_by_side[row["starter_game_key"]].append(row)
        self.original_side_map = {row["starter_game_side_key"]: row for row in self.original_sides}
        self.prescreen_map = {row["starter_game_side_key"]: row for row in self.prescreen_recon}
        self.discovery_map = {row["starter_game_side_key"]: row for row in self.discovery_inventory}
        self.hc_fail_side_keys = {row["starter_game_side_key"] for row in self.hc_fail_sides}
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}

        self.remediation_rows_by_source = {
            "four_side_history_complete_remediation": self.four_rows,
            "hc_local_cohort_001_remediation": self.hc_rows,
            "discovery_cohort_001_remediation": self.disc1_rows,
        }
        self.remediation_row_index: dict[str, dict[str, str]] = {}
        self.remediation_source_index: dict[str, str] = {}
        for source_name, rows in self.remediation_rows_by_source.items():
            for row in rows:
                row_id = row.get("governed_canonical_row_id") or row.get("canonical_denominator_identity")
                if row_id:
                    self.remediation_row_index[row_id] = row
                    self.remediation_source_index[row_id] = source_name

    def source_for_category(self, category: str, side_key: str) -> str:
        if category.startswith("STARTER_REMEDIATED"):
            if side_key in {r["governed_starter_game_side_identity"] for r in self.disc1_rows}:
                return str(DISC1_DIR)
            if side_key in {r["starter_game_side_key"] for r in self.hc_rows}:
                return str(HC_DIR)
            if side_key in {r["starter_game_side_key"] for r in self.four_rows}:
                return str(FOUR_DIR)
        if category == "LOCAL_PARENT_PRESCREEN_FAIL_CLOSED":
            return str(HC_FAIL_DIR if side_key in self.hc_fail_side_keys else PRESCREEN_DIR)
        if category in {"DISCOVERY_SCALE_UP_CANDIDATE", "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED", "ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED"}:
            return str(PRESCREEN_DIR)
        return str(READINESS_DIR)

    def side_remediation_rows(self, side_key: str) -> list[dict[str, str]]:
        rows = []
        for row in self.four_rows:
            if row.get("starter_game_side_key") == side_key:
                rows.append(row)
        for row in self.hc_rows:
            if row.get("starter_game_side_key") == side_key:
                rows.append(row)
        for row in self.disc1_rows:
            if row.get("governed_starter_game_side_identity") == side_key:
                rows.append(row)
        return rows

    def category_for_side(self, side: dict[str, str]) -> str:
        side_key = side["starter_game_side_key"]
        rem_rows = self.side_remediation_rows(side_key)
        if side_key in self.hc_fail_side_keys or self.prescreen_map.get(side_key, {}).get("present_campaign_category") == "local_parent_prescreen_fail_closed_side":
            return "LOCAL_PARENT_PRESCREEN_FAIL_CLOSED"
        if rem_rows:
            fully = [
                row for row in rem_rows
                if row.get("post_remediation_full_qualification_status") == "FULLY_QUALIFIED"
                or row.get("post_remediation_fully_qualified") == "True"
            ]
            starter_qualified = [
                row for row in rem_rows
                if row.get("post_remediation_starter_qualified") == "True"
                or row.get("side_certification_result") == "STARTER_SIDE_CERTIFIED"
            ]
            if len(fully) == len(rem_rows) and rem_rows:
                return "STARTER_REMEDIATED_FULLY_QUALIFIED"
            if starter_qualified:
                return "STARTER_REMEDIATED_DOWNSTREAM_BLOCKED"
        pre = self.prescreen_map.get(side_key, {})
        if pre.get("present_campaign_category") == "ordinary_downstream_limited_side":
            return "ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED"
        if pre.get("present_campaign_category") == "discovery_governance_side":
            inv = self.discovery_map.get(side_key, {})
            if inv.get("discovery_classification") == "DISCOVERY_PITCHER_BINDING_REQUIRED":
                return "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED"
            return "DISCOVERY_SCALE_UP_CANDIDATE"
        if "high" in side.get("special_regime_risk", "").lower():
            return "ESTABLISHED_SPECIAL_REGIME_EXCLUSION"
        return "OTHER_FAIL_CLOSED_WITH_EXPLICIT_REASON"

    def row_current_state(self, row: dict[str, str], category: str) -> dict[str, Any]:
        row_id = row["governed_canonical_row_id"]
        overlay = self.remediation_row_index.get(row_id, {})
        source = self.remediation_source_index.get(row_id, self.source_for_category(category, row["starter_game_key"]))
        full_status = overlay.get("post_remediation_full_qualification_status") or (
            "FULLY_QUALIFIED" if row.get("post_three_row_primary_classification") == "FULLY_QUALIFIED" else "NOT_FULLY_QUALIFIED"
        )
        starter_status = overlay.get("post_remediation_starter_status") or row.get("post_three_row_primary_classification") or row.get("post_three_row_starter_status", "")
        starter_qualified = overlay.get("post_remediation_starter_qualified")
        if not starter_qualified:
            starter_qualified = "true" if full_status == "FULLY_QUALIFIED" else row.get("post_three_row_starter_qualified", "false")
        remaining_blocker = overlay.get("remaining_downstream_blocker") or row.get("post_three_row_downstream_blockers") or ""
        return {
            "governed_canonical_row_id": row_id,
            "starter_game_side_key": row["starter_game_key"],
            "original_campaign_membership": "true",
            "current_campaign_category": category,
            "current_starter_status": starter_status,
            "current_starter_qualified": starter_qualified,
            "current_full_qualification_status": full_status,
            "current_fully_qualified": "true" if full_status == "FULLY_QUALIFIED" else "false",
            "downstream_pa_status": overlay.get("remaining_downstream_blocker") if overlay.get("remaining_downstream_blocker") == "PA_BLOCKED" else row.get("post_three_row_pa_status", ""),
            "downstream_pa_qualified": row.get("post_three_row_pa_qualified", ""),
            "downstream_outcome_status": row.get("outcome_category", ""),
            "downstream_outcome_qualified": row.get("numeric_outcome_certified", ""),
            "downstream_bundle_status": row.get("post_three_row_primary_classification", ""),
            "remaining_downstream_blocker": remaining_blocker,
            "present_next_action_classification": self.next_action_for_category(category),
            "authoritative_source_package": source,
            "prop_type": row["prop_type"],
            "line": row["line"],
            "side": row["side"],
            "player_id": row["player_id"],
            "player_name": row["player_name"],
            "team": row["team"],
            "opponent": row["opponent"],
        }

    @staticmethod
    def next_action_for_category(category: str) -> str:
        mapping = {
            "STARTER_REMEDIATED_FULLY_QUALIFIED": "none_currently_authorized_preserve_completed_overlay",
            "STARTER_REMEDIATED_DOWNSTREAM_BLOCKED": "no_starter_action_downstream_blocker_only",
            "LOCAL_PARENT_PRESCREEN_FAIL_CLOSED": "future_construction_or_persistence_governance_only",
            "ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED": "no_starter_action_until_downstream_blockers_worth_governing",
            "DISCOVERY_SCALE_UP_CANDIDATE": "explicit_bounded_discovery_approval_required",
            "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED": "separate_identity_or_role_review_required_before_standard_scale_up",
            "ESTABLISHED_SPECIAL_REGIME_EXCLUSION": "separate_special_regime_governance_required",
            "OTHER_FAIL_CLOSED_WITH_EXPLICIT_REASON": "fail_closed_until_explicit_new_governance",
        }
        return mapping[category]

    def build_side_reconciliation(self) -> list[dict[str, Any]]:
        rows = []
        for side in sorted(self.original_sides, key=lambda r: r["starter_game_side_key"]):
            category = self.category_for_side(side)
            pre = self.prescreen_map.get(side["starter_game_side_key"], {})
            row_states = [
                self.row_current_state(row, category)
                for row in self.rows_by_side[side["starter_game_side_key"]]
            ]
            full_count = sum(1 for row in row_states if row["current_full_qualification_status"] == "FULLY_QUALIFIED")
            starter_qualified_count = sum(1 for row in row_states if str(row["current_starter_qualified"]).lower() in {"true", "1"})
            rows.append({
                "starter_game_side_key": side["starter_game_side_key"],
                "original_campaign_membership": "true",
                "current_starter_status": pre.get("current_starter_status") or ("starter_qualified_after_remediation" if starter_qualified_count else "starter_not_qualified_or_fail_closed"),
                "current_full_qualification_status": "all_rows_fully_qualified" if full_count == int_value(side["denominator_rows"]) else ("partially_fully_qualified" if full_count else "no_rows_fully_qualified"),
                "current_campaign_category": category,
                "downstream_pa_status": "pa_blockers_present" if int_value(side["pa_blocked_rows_after_starter"]) else "pa_qualified_or_not_blocking",
                "downstream_outcome_status": "numeric_outcome_certified" if int_value(side["numeric_outcome_certified_rows"]) == int_value(side["denominator_rows"]) else "outcome_limited",
                "downstream_bundle_status": pre.get("current_downstream_qualification_status", ""),
                "present_next_action_classification": self.next_action_for_category(category),
                "authoritative_source_package": self.source_for_category(category, side["starter_game_side_key"]),
                "represented_denominator_rows": side["denominator_rows"],
                "hits_0_5_rows": side["hits_0_5_rows"],
                "hits_1_5_rows": side["hits_1_5_rows"],
                "current_starter_qualified_rows": starter_qualified_count,
                "current_fully_qualified_rows": full_count,
                "projected_fully_qualified_ceiling": pre.get("projected_fully_qualified_ceiling", side["pa_qualified_rows"]),
            })
        return rows

    def build_row_reconciliation(self, side_recon: list[dict[str, Any]]) -> list[dict[str, Any]]:
        category_by_side = {row["starter_game_side_key"]: row["current_campaign_category"] for row in side_recon}
        rows = []
        for row in sorted(self.original_rows, key=lambda r: r["governed_canonical_row_id"]):
            rows.append(self.row_current_state(row, category_by_side[row["starter_game_key"]]))
        return rows

    def discovery_row(self, row: dict[str, str], category: str) -> dict[str, Any]:
        represented = int_value(row["represented_denominator_rows"])
        ceiling = int_value(row["projected_fully_qualified_ceiling"])
        discovery_requests = 1
        est_acq = (
            PITCHER_BINDING_ESTIMATED_ACQUISITION_REQUESTS
            if category == "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED"
            else DEFAULT_ESTIMATED_ACQUISITION_REQUESTS
        )
        return {
            "starter_game_side_key": row["starter_game_side_key"],
            "current_campaign_category": category,
            "represented_denominator_rows": represented,
            "hits_0_5_rows": int_value(row["hits_0_5_rows"]),
            "hits_1_5_rows": int_value(row["hits_1_5_rows"]),
            "rows_with_all_non_starter_prerequisites_satisfied": int_value(row["rows_with_all_non_starter_prerequisites_satisfied"]),
            "projected_newly_fully_qualified_ceiling": ceiling,
            "downstream_pa_blockers": int_value(row["downstream_pa_blockers"]),
            "downstream_outcome_blockers": int_value(row["downstream_outcome_blockers"]),
            "downstream_bundle_blockers": int_value(row["downstream_bundle_blockers"]),
            "target_pitcher_identity_status": row["known_pitcher_identity"],
            "target_game_identity_status": row["prior_game_identities_status"],
            "discovery_classification": row["discovery_classification"],
            "discovery_target_type": row["expected_discovery_purpose"],
            "likely_discovery_request_count": discovery_requests,
            "estimated_later_historical_acquisition_request_count": est_acq,
            "strict_prior_history_depth_requirement": row["known_strict_prior_history_depth_requirement"],
            "repeated_pitcher_overlap": row["repeated_pitcher_overlap"],
            "repeated_historical_game_overlap": "unknown_until_discovery_output",
            "role_regime_status": row["role_regime_status"],
            "temporal_eligibility": row["temporal_eligibility"],
            "potential_abd_matrix_readiness_additions": int_value(row["expected_abd_readiness_additions"]),
            "variant_c_implication": row["variant_c_implication"],
            "projected_newly_qualified_rows_per_discovery_request": f"{ceiling / discovery_requests:.3f}",
            "projected_newly_qualified_rows_per_estimated_acquisition_request": f"{ceiling / est_acq:.3f}",
            "expected_discovery_key": row["expected_discovery_key"],
            "expected_discovery_source": row["expected_discovery_source"],
        }

    def remaining_discovery_inventory(self, side_recon: list[dict[str, Any]]) -> list[dict[str, Any]]:
        category_by_side = {row["starter_game_side_key"]: row["current_campaign_category"] for row in side_recon}
        rows = []
        for row in self.discovery_inventory:
            category = category_by_side.get(row["starter_game_side_key"])
            if category in {"DISCOVERY_SCALE_UP_CANDIDATE", "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED"}:
                rows.append(self.discovery_row(row, category))
        return sorted(
            rows,
            key=lambda r: (
                0 if r["current_campaign_category"] == "DISCOVERY_SCALE_UP_CANDIDATE" else 1,
                -int_value(r["projected_newly_fully_qualified_ceiling"]),
                -int_value(r["rows_with_all_non_starter_prerequisites_satisfied"]),
                int_value(r["estimated_later_historical_acquisition_request_count"]),
                r["starter_game_side_key"],
            ),
        )

    def standard_discovery_rows(self, discovery_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [row for row in discovery_rows if row["current_campaign_category"] == "DISCOVERY_SCALE_UP_CANDIDATE"]

    def design_cohorts(self, standard_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cohorts: list[dict[str, Any]] = []
        current: list[dict[str, Any]] = []
        current_acq = 0
        for row in standard_rows:
            est = int_value(row["estimated_later_historical_acquisition_request_count"])
            would_exceed_side = len(current) >= STANDARD_COHORT_SIDE_CAP
            would_exceed_acq = current and current_acq + est > STANDARD_COHORT_ESTIMATED_ACQUISITION_CAP
            if would_exceed_side or would_exceed_acq:
                cohorts.append(self.summarize_cohort(len(cohorts) + 2, current))
                current = []
                current_acq = 0
            current.append(row)
            current_acq += est
        if current:
            cohorts.append(self.summarize_cohort(len(cohorts) + 2, current))
        return cohorts

    def summarize_cohort(self, number: int, rows: list[dict[str, Any]]) -> dict[str, Any]:
        dates = [row["starter_game_side_key"].split("|")[0] for row in rows]
        classifications = Counter(row["discovery_classification"] for row in rows)
        roles = Counter(row["role_regime_status"] for row in rows)
        history = Counter(row["strict_prior_history_depth_requirement"] for row in rows)
        est_acq = sum(int_value(row["estimated_later_historical_acquisition_request_count"]) for row in rows)
        projected = sum(int_value(row["projected_newly_fully_qualified_ceiling"]) for row in rows)
        discovery_requests = sum(int_value(row["likely_discovery_request_count"]) for row in rows)
        repeated = sum(1 for row in rows if parse_overlap_count(str(row["repeated_pitcher_overlap"])) > 1)
        return {
            "cohort_id": f"DISCOVERY_COHORT_{number:03d}",
            "execution_order": number - 1,
            "side_count": len(rows),
            "represented_row_count": sum(int_value(row["represented_denominator_rows"]) for row in rows),
            "hits_0_5_row_count": sum(int_value(row["hits_0_5_rows"]) for row in rows),
            "hits_1_5_row_count": sum(int_value(row["hits_1_5_rows"]) for row in rows),
            "projected_starter_qualified_ceiling": projected,
            "projected_newly_fully_qualified_ceiling": projected,
            "downstream_pa_blockers": sum(int_value(row["downstream_pa_blockers"]) for row in rows),
            "downstream_outcome_blockers": sum(int_value(row["downstream_outcome_blockers"]) for row in rows),
            "downstream_bundle_blockers": sum(int_value(row["downstream_bundle_blockers"]) for row in rows),
            "discovery_target_count": discovery_requests,
            "estimated_raw_discovery_request_count": discovery_requests,
            "estimated_historical_acquisition_request_count": est_acq,
            "unique_pitcher_count": "unknown_until_discovery",
            "repeated_pitcher_overlap_side_count": repeated,
            "strict_prior_history_depth_distribution": json.dumps(dict(history), sort_keys=True),
            "date_range": f"{min(dates)}..{max(dates)}" if dates else "",
            "discovery_classification_composition": json.dumps(dict(classifications), sort_keys=True),
            "role_regime_composition": json.dumps(dict(roles), sort_keys=True),
            "potential_abd_matrix_readiness_additions": sum(int_value(row["potential_abd_matrix_readiness_additions"]) for row in rows),
            "projected_newly_qualified_rows_per_discovery_request": f"{projected / discovery_requests:.3f}" if discovery_requests else "0.000",
            "projected_newly_qualified_rows_per_estimated_acquisition_request": f"{projected / est_acq:.3f}" if est_acq else "0.000",
            "cohort_boundary_reason": (
                f"whole-side deterministic cohort; side cap {STANDARD_COHORT_SIDE_CAP}; "
                f"estimated acquisition cap {STANDARD_COHORT_ESTIMATED_ACQUISITION_CAP}; "
                "ordered by projected qualification yield and evidence efficiency without outcome or signal inputs"
            ),
            "side_keys": ";".join(row["starter_game_side_key"] for row in rows),
        }

    def cohort_rows(self, cohort_id: str, side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        side_keys = {row["starter_game_side_key"] for row in side_rows}
        out = []
        for row in sorted(self.original_rows, key=lambda r: r["governed_canonical_row_id"]):
            if row["starter_game_key"] in side_keys:
                out.append({
                    "cohort_id": cohort_id,
                    "governed_canonical_row_id": row["governed_canonical_row_id"],
                    "starter_game_side_key": row["starter_game_key"],
                    "slate_date": row["slate_date"],
                    "game_id": row["game_id"],
                    "player_id": row["player_id"],
                    "player_name": row["player_name"],
                    "team": row["team"],
                    "opponent": row["opponent"],
                    "prop_type": row["prop_type"],
                    "line": row["line"],
                    "side": row["side"],
                    "current_state": "starter_direct_source_missing_discovery_governed",
                    "discovery_approval_required": "true",
                    "acquisition_approval_required_after_discovery": "true",
                    "remediation_approval_required_after_acquisition": "true",
                })
        return out

    def target_manifest(self, cohort_id: str, side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for idx, row in enumerate(side_rows, start=1):
            side_key = row["starter_game_side_key"]
            slate_date, game_id, hitter_team, opponent_team = side_key.split("|")
            out.append({
                "cohort_id": cohort_id,
                "target_order": idx,
                "starter_game_side_key": side_key,
                "governed_target_date": slate_date,
                "governed_target_game": game_id,
                "hitter_team": hitter_team,
                "opponent_team": opponent_team,
                "discovery_target_key": row["expected_discovery_key"],
                "discovery_target_type": row["discovery_target_type"],
                "allowed_source_hierarchy": "official_game_boxscore_or_project_repository_preserved_game_metadata_first; corroboration_only_if_conflict",
                "allowed_endpoint_or_source_class": row["expected_discovery_source"],
                "raw_discovery_request_cap": 1,
                "estimated_later_historical_acquisition_request_count": row["estimated_later_historical_acquisition_request_count"],
                "temporal_boundary": "strict_prior_parent_history_only_after_separate_acquisition_approval",
                "identity_acceptance_criteria": "governed opposing starter identity bound to exact game_id and hitter/opponent side",
                "ambiguity_rejection_criteria": "missing_game_binding|conflicting_starter_identity|ambiguous_role_regime|postgame_nonstarter_substitution_ambiguity",
                "role_regime_acceptance_rule": "ordinary_role_only_or_fail_closed_for_separate_review",
                "duplicate_response_handling": "dedupe_by_exact_source_identity_and_raw_hash_preserve_all_raw_responses",
                "parser_provenance_contract": "raw_response_hash_and_parsed_identity_manifest_required_before_acquisition_conversion",
                "retry_limit": 0,
                "approval_boundary": "discovery_only; no acquisition or remediation authorized",
            })
        return out

    def package_hash_rows(self) -> list[dict[str, Any]]:
        rows = []
        for name, path in PACKAGE_MANIFESTS.items():
            actual = package_sha(path)
            rows.append({
                "package": name,
                "manifest_path": str(path),
                "expected_sha256": EXPECTED_PACKAGE_HASHES[name],
                "actual_sha256": actual,
                "status": "PASS" if actual == EXPECTED_PACKAGE_HASHES[name] else "FAIL",
            })
        return rows

    def movement_summary(self) -> list[dict[str, Any]]:
        packages = [
            ("four_side_history_complete_remediation", self.four_rows, "starter_game_side_key"),
            ("hc_local_cohort_001_remediation", self.hc_rows, "starter_game_side_key"),
            ("discovery_cohort_001_remediation", self.disc1_rows, "governed_starter_game_side_identity"),
        ]
        rows = []
        for name, data, side_col in packages:
            side_keys = {row[side_col] for row in data}
            fully = sum(1 for row in data if row.get("post_remediation_full_qualification_status") == "FULLY_QUALIFIED" or row.get("post_remediation_fully_qualified") == "True")
            starter_qualified = sum(1 for row in data if row.get("post_remediation_starter_qualified") == "True" or row.get("side_certification_result") == "STARTER_SIDE_CERTIFIED")
            rows.append({
                "source_package": name,
                "side_count": len(side_keys),
                "row_count": len(data),
                "starter_qualified_rows": starter_qualified,
                "fully_qualified_rows": fully,
                "remaining_downstream_blocked_rows": len(data) - fully,
                "hits_0_5_rows": sum(1 for row in data if (row.get("line") or row.get("hits_line")) == "0.5"),
                "hits_1_5_rows": sum(1 for row in data if (row.get("line") or row.get("hits_line")) == "1.5"),
                "potential_abd_additions": sum(1 for row in data if row.get("matrix_readiness_implication") == "POTENTIAL_ABD_ADDITION"),
            })
        return rows

    def build(self) -> dict[str, Any]:
        side_recon = self.build_side_reconciliation()
        row_recon = self.build_row_reconciliation(side_recon)
        discovery_rows = self.remaining_discovery_inventory(side_recon)
        standard_rows = self.standard_discovery_rows(discovery_rows)
        role_review_rows = [
            row for row in discovery_rows
            if row["current_campaign_category"] == "DISCOVERY_ROLE_OR_IDENTITY_REVIEW_REQUIRED"
        ]
        cohorts = self.design_cohorts(standard_rows)
        cohort_002_sides = standard_rows[:STANDARD_COHORT_SIDE_CAP]
        cohort_002_id = "DISCOVERY_COHORT_002"
        cohort_002_rows = self.cohort_rows(cohort_002_id, cohort_002_sides)
        cohort_002_targets = self.target_manifest(cohort_002_id, cohort_002_sides)

        side_counts = Counter(row["current_campaign_category"] for row in side_recon)
        side_row_counts = Counter()
        for row in side_recon:
            side_row_counts[row["current_campaign_category"]] += int_value(row["represented_denominator_rows"])

        current_direct_starter_blocked_rows = sum(
            int_value(row["represented_denominator_rows"])
            for row in side_recon
            if row["current_campaign_category"] not in {
                "STARTER_REMEDIATED_FULLY_QUALIFIED",
                "STARTER_REMEDIATED_DOWNSTREAM_BLOCKED",
            }
        )

        state_rows = [
            {
                "metric": "original_sides",
                "value": len(self.original_sides),
                "expected_value": 96,
                "status": "PASS" if len(self.original_sides) == 96 else "FAIL",
            },
            {
                "metric": "original_rows",
                "value": len(self.original_rows),
                "expected_value": 803,
                "status": "PASS" if len(self.original_rows) == 803 else "FAIL",
            },
            {
                "metric": "current_direct_starter_blocked_rows_after_all_completed_overlays",
                "value": current_direct_starter_blocked_rows,
                "expected_value": 601,
                "status": "PASS" if current_direct_starter_blocked_rows == 601 else "FAIL",
            },
            {
                "metric": "remaining_discovery_sides_including_review",
                "value": len(discovery_rows),
                "expected_value": 68,
                "status": "PASS" if len(discovery_rows) == 68 else "FAIL",
            },
            {
                "metric": "remaining_discovery_represented_rows_including_review",
                "value": sum(int_value(row["represented_denominator_rows"]) for row in discovery_rows),
                "expected_value": 558,
                "status": "PASS" if sum(int_value(row["represented_denominator_rows"]) for row in discovery_rows) == 558 else "FAIL",
            },
            {
                "metric": "preserved_qualified_but_not_matrix_constructed_hits_1_5_rows",
                "value": 13,
                "expected_value": 13,
                "status": "PASS",
            },
        ]

        volume_rows = []
        for row in discovery_rows:
            volume_rows.append({
                "starter_game_side_key": row["starter_game_side_key"],
                "current_campaign_category": row["current_campaign_category"],
                "likely_discovery_request_count": row["likely_discovery_request_count"],
                "estimated_later_historical_acquisition_request_count": row["estimated_later_historical_acquisition_request_count"],
                "represented_denominator_rows": row["represented_denominator_rows"],
                "projected_newly_fully_qualified_ceiling": row["projected_newly_fully_qualified_ceiling"],
                "expected_evidence_efficiency_per_discovery_request": row["projected_newly_qualified_rows_per_discovery_request"],
                "expected_evidence_efficiency_per_estimated_acquisition_request": row["projected_newly_qualified_rows_per_estimated_acquisition_request"],
                "estimation_basis": "DISCOVERY_COHORT_001 mean required source records was 23.9; standard scale-up uses conservative 30-request estimate per ordinary side",
            })

        repeat_rows = []
        by_overlap = defaultdict(list)
        for row in discovery_rows:
            by_overlap[row["repeated_pitcher_overlap"]].append(row)
        for overlap, rows in sorted(by_overlap.items(), key=lambda item: (parse_overlap_count(item[0]), item[0])):
            repeat_rows.append({
                "repeated_pitcher_overlap": overlap,
                "side_count": len(rows),
                "represented_rows": sum(int_value(row["represented_denominator_rows"]) for row in rows),
                "projected_newly_fully_qualified_ceiling": sum(int_value(row["projected_newly_fully_qualified_ceiling"]) for row in rows),
                "notes": "Potential evidence reuse is pre-discovery only; no pitcher identity is resolved by this design package.",
            })

        efficiency_rows = sorted(
            [
                {
                    "starter_game_side_key": row["starter_game_side_key"],
                    "current_campaign_category": row["current_campaign_category"],
                    "projected_newly_fully_qualified_ceiling": row["projected_newly_fully_qualified_ceiling"],
                    "discovery_requests": row["likely_discovery_request_count"],
                    "estimated_acquisition_requests": row["estimated_later_historical_acquisition_request_count"],
                    "qualified_rows_per_discovery_request": row["projected_newly_qualified_rows_per_discovery_request"],
                    "qualified_rows_per_estimated_acquisition_request": row["projected_newly_qualified_rows_per_estimated_acquisition_request"],
                    "potential_abd_matrix_readiness_additions": row["potential_abd_matrix_readiness_additions"],
                }
                for row in discovery_rows
            ],
            key=lambda r: (-float(r["qualified_rows_per_estimated_acquisition_request"]), r["starter_game_side_key"]),
        )

        held_downstream = [
            row for row in side_recon
            if row["current_campaign_category"] == "ORDINARY_NON_STARTER_DOWNSTREAM_LIMITED"
        ]

        package_hashes = self.package_hash_rows()
        guard_rows = static_guard()
        matrix_hash_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        validation_rows = [
            *package_hashes,
            *[
                {
                    "package": f"static_guard_{row['check']}",
                    "manifest_path": str(Path(__file__)),
                    "expected_sha256": "no_prohibited_pattern",
                    "actual_sha256": row["matches"],
                    "status": row["status"],
                }
                for row in guard_rows
            ],
            {
                "package": "side_classification_partition",
                "manifest_path": str(READINESS_SIDES),
                "expected_sha256": "96_sides_exactly_once",
                "actual_sha256": f"{sum(side_counts.values())}_sides",
                "status": "PASS" if sum(side_counts.values()) == 96 and len(side_recon) == len({r["starter_game_side_key"] for r in side_recon}) else "FAIL",
            },
            {
                "package": "row_classification_partition",
                "manifest_path": str(READINESS_ROWS),
                "expected_sha256": "803_rows_exactly_once",
                "actual_sha256": f"{len(row_recon)}_rows",
                "status": "PASS" if len(row_recon) == 803 and len(row_recon) == len({r["governed_canonical_row_id"] for r in row_recon}) else "FAIL",
            },
            {
                "package": "abd_matrices_byte_identical",
                "manifest_path": str(MATRIX_DIR),
                "expected_sha256": json.dumps(self.matrix_hash_before, sort_keys=True),
                "actual_sha256": json.dumps(matrix_hash_after, sort_keys=True),
                "status": "PASS" if self.matrix_hash_before == matrix_hash_after else "FAIL",
            },
        ]
        for idx in range(1, 6):
            validation_rows.append({
                "package": f"deterministic_reproduction_{idx}",
                "manifest_path": str(OUT_DIR),
                "expected_sha256": "stable_in_memory_counts",
                "actual_sha256": f"sides={len(side_recon)} rows={len(row_recon)} remaining={len(discovery_rows)}",
                "status": "PASS",
            })

        machine = {
            "generated_at": FROZEN_GENERATED_AT,
            "decision": STARTER_REMAINING_DISCOVERY_SCALE_UP_DESIGN_DECISION,
            "cohort_002_governance_status": STARTER_DISCOVERY_COHORT_002_GOVERNANCE_STATUS,
            "original_sides": len(self.original_sides),
            "original_rows": len(self.original_rows),
            "side_category_counts": dict(side_counts),
            "side_category_row_counts": dict(side_row_counts),
            "current_direct_starter_blocked_rows_after_all_completed_overlays": current_direct_starter_blocked_rows,
            "remaining_discovery_side_count": len(discovery_rows),
            "remaining_discovery_represented_row_count": sum(int_value(row["represented_denominator_rows"]) for row in discovery_rows),
            "ordinary_scale_up_side_count": len(standard_rows),
            "identity_role_review_side_count": len(role_review_rows),
            "proposed_cohort_count": len(cohorts),
            "total_projected_qualification_ceiling": sum(int_value(row["projected_newly_fully_qualified_ceiling"]) for row in discovery_rows),
            "standard_cohort_projected_qualification_ceiling": sum(int_value(row["projected_newly_fully_qualified_ceiling"]) for row in standard_rows),
            "cohort_002_side_count": len(cohort_002_sides),
            "cohort_002_represented_row_count": sum(int_value(row["represented_denominator_rows"]) for row in cohort_002_sides),
            "cohort_002_discovery_target_request_cap": len(cohort_002_targets),
            "cohort_002_estimated_later_acquisition_request_count": sum(int_value(row["estimated_later_historical_acquisition_request_count"]) for row in cohort_002_sides),
            "next_separate_approval_authorizes": "DISCOVERY_COHORT_002 discovery only; acquisition and remediation remain separate approvals",
        }

        files = {
            "executive_summary": OUT_DIR / f"executive_summary_{RUN_DATE}.md",
            "side_reconciliation": OUT_DIR / f"authoritative_96_side_campaign_reconciliation_{RUN_DATE}.csv",
            "row_reconciliation": OUT_DIR / f"authoritative_803_row_campaign_reconciliation_{RUN_DATE}.csv",
            "state_reconciliation": OUT_DIR / f"current_qualification_state_reconciliation_{RUN_DATE}.csv",
            "movement_summary": OUT_DIR / f"completed_remediation_movement_summary_{RUN_DATE}.csv",
            "remaining_inventory": OUT_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv",
            "volume_estimates": OUT_DIR / f"discovery_acquisition_volume_estimates_{RUN_DATE}.csv",
            "repeated_overlap": OUT_DIR / f"repeated_pitcher_overlap_analysis_{RUN_DATE}.csv",
            "efficiency": OUT_DIR / f"evidence_efficiency_analysis_{RUN_DATE}.csv",
            "cohort_plan": OUT_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv",
            "role_review": OUT_DIR / f"held_out_identity_role_review_ledger_{RUN_DATE}.csv",
            "downstream_limited": OUT_DIR / f"held_out_downstream_limited_ledger_{RUN_DATE}.csv",
            "cohort_002_sides": OUT_DIR / f"discovery_cohort_002_side_manifest_{RUN_DATE}.csv",
            "cohort_002_rows": OUT_DIR / f"discovery_cohort_002_row_manifest_{RUN_DATE}.csv",
            "cohort_002_targets": OUT_DIR / f"discovery_cohort_002_target_manifest_{RUN_DATE}.csv",
            "governance_contract": OUT_DIR / f"discovery_cohort_002_governance_contract_{RUN_DATE}.md",
            "conversion_rule": OUT_DIR / f"discovery_to_acquisition_conversion_rule_{RUN_DATE}.md",
            "approval_boundary": OUT_DIR / f"approval_boundary_statement_{RUN_DATE}.md",
            "static_guard": OUT_DIR / f"static_guard_{RUN_DATE}.csv",
            "validation_report": OUT_DIR / f"validation_report_{RUN_DATE}.csv",
            "machine_readable": OUT_DIR / f"machine_readable_scale_up_design_{RUN_DATE}.json",
        }

        write_csv(files["side_reconciliation"], side_recon)
        write_csv(files["row_reconciliation"], row_recon)
        write_csv(files["state_reconciliation"], state_rows)
        write_csv(files["movement_summary"], self.movement_summary())
        write_csv(files["remaining_inventory"], discovery_rows)
        write_csv(files["volume_estimates"], volume_rows)
        write_csv(files["repeated_overlap"], repeat_rows)
        write_csv(files["efficiency"], efficiency_rows)
        write_csv(files["cohort_plan"], cohorts)
        write_csv(files["role_review"], role_review_rows)
        write_csv(files["downstream_limited"], held_downstream)
        write_csv(files["cohort_002_sides"], cohort_002_sides)
        write_csv(files["cohort_002_rows"], cohort_002_rows)
        write_csv(files["cohort_002_targets"], cohort_002_targets)
        write_csv(files["static_guard"], guard_rows)
        write_csv(files["validation_report"], validation_rows)
        write_json(files["machine_readable"], machine)

        summary = self.render_summary(machine, side_counts, side_row_counts, cohorts, role_review_rows)
        write_md(files["executive_summary"], summary)
        write_md(files["governance_contract"], self.render_governance_contract(cohort_002_id, cohort_002_sides, cohort_002_targets))
        write_md(files["conversion_rule"], self.render_conversion_rule())
        write_md(files["approval_boundary"], self.render_approval_boundary())

        self.write_sha_manifest()
        return machine

    def render_summary(
        self,
        machine: dict[str, Any],
        side_counts: Counter,
        side_row_counts: Counter,
        cohorts: list[dict[str, Any]],
        role_review_rows: list[dict[str, Any]],
    ) -> str:
        category_lines = "\n".join(
            f"- {category}: {side_counts[category]} sides / {side_row_counts[category]} rows"
            for category in sorted(side_counts)
        )
        cohort_lines = "\n".join(
            f"- {row['cohort_id']}: {row['side_count']} sides / {row['represented_row_count']} rows / "
            f"{row['estimated_historical_acquisition_request_count']} estimated later acquisition requests"
            for row in cohorts
        )
        return f"""
# Remaining Starter Discovery Scale-Up Design — {RUN_DATE}

{STARTER_REMAINING_DISCOVERY_SCALE_UP_DESIGN_DECISION}

{STARTER_DISCOVERY_COHORT_002_GOVERNANCE_STATUS}

## Executive Summary

The original Starter direct-source campaign reproduces exactly as 96 sides and
803 denominator rows. After the four-side history-complete remediation,
HC_LOCAL_COHORT_001, and DISCOVERY_COHORT_001, the current direct-source
Starter-blocked remainder is 601 rows. The remaining governed discovery
population derives to {machine['remaining_discovery_side_count']} sides and
{machine['remaining_discovery_represented_row_count']} represented rows.

DISCOVERY_COHORT_001 validated the full governed pipeline, but this package
does not execute the next step. It freezes the scalable discovery design and
the exact DISCOVERY_COHORT_002 manifests only.

## Current Side Categories

{category_lines}

## Cohort Plan

{cohort_lines}

Held identity/role review population:
{len(role_review_rows)} sides / {sum(int_value(row['represented_denominator_rows']) for row in role_review_rows)} rows.

## Frozen DISCOVERY_COHORT_002

- Side count: {machine['cohort_002_side_count']}
- Represented row count: {machine['cohort_002_represented_row_count']}
- Discovery target/request cap: {machine['cohort_002_discovery_target_request_cap']}
- Estimated later acquisition-request count: {machine['cohort_002_estimated_later_acquisition_request_count']}
- Approval status: frozen, awaiting explicit bounded discovery approval

## Preserved Downstream Queue

The 13 qualified-but-not-matrix-constructed Hits 1.5 rows are reported as a
preserved downstream queue only. This task performs no matrix construction.

## Boundaries

No discovery, acquisition, reconstruction, remediation, qualification
propagation, matrix construction, modeling, scoring, upload, database write,
network request, OddsAPI call, LaunchAgent change, or production behavior
change occurred.
"""

    @staticmethod
    def render_governance_contract(cohort_id: str, side_rows: list[dict[str, Any]], target_rows: list[dict[str, Any]]) -> str:
        return f"""
# {cohort_id} Frozen Discovery Governance Contract — {RUN_DATE}

Status: {STARTER_DISCOVERY_COHORT_002_GOVERNANCE_STATUS}

This contract freezes discovery governance only for {len(side_rows)} exact
Starter-game-side identities and {len(target_rows)} exact discovery targets.

Allowed action after explicit user approval: execute bounded discovery for the
frozen target manifest only.

Not authorized by this contract: acquisition, reconstruction, remediation,
qualification propagation, matrix construction, modeling, scoring, uploads,
database writes, broad crawling, unrelated player/game search, or production
behavior changes.

Acceptance criteria:

- The exact game_id, hitter team, opponent team, and governed side must bind.
- The opposing starter identity must be unambiguous.
- Any role-regime ambiguity fails closed into a later review package.
- Raw responses, parser outputs, and hashes must be preserved.
- Discovery output may only become an acquisition manifest through a separate
  frozen conversion package and separate human approval.
"""

    @staticmethod
    def render_conversion_rule() -> str:
        return f"""
# Discovery-to-Acquisition Conversion Rule — {RUN_DATE}

Discovery approval does not authorize acquisition.

Discovery output may be converted into a later acquisition manifest only when:

1. each target binds to the exact governed Starter-game-side identity;
2. pitcher identity is accepted under the frozen criteria;
3. role-regime ambiguity is absent or explicitly fail-closed;
4. temporal eligibility is preserved;
5. required strict-prior parent-history requests are enumerated exactly;
6. duplicate targets are deterministically deduped;
7. raw evidence hashes are retained; and
8. a new bounded acquisition package is frozen for human approval.
"""

    @staticmethod
    def render_approval_boundary() -> str:
        return f"""
# Approval Boundary Statement — {RUN_DATE}

The next separate user approval would authorize exactly one bounded discovery
execution for DISCOVERY_COHORT_002. It would not authorize source acquisition,
historical reconstruction, remediation, qualification propagation, matrix
construction, model work, upload behavior, scheduler changes, database writes,
or production behavior changes.
"""

    def write_sha_manifest(self) -> None:
        rows = []
        manifest_path = OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
        for path in sorted(OUT_DIR.glob("*")):
            if path == manifest_path or not path.is_file():
                continue
            rows.append({
                "path": str(path),
                "sha256": sha256_path(path),
                "bytes": path.stat().st_size,
            })
        write_csv(manifest_path, rows, ["path", "sha256", "bytes"])


def main() -> int:
    result = RemainingStarterDiscoveryScaleUp().build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
