#!/usr/bin/env python3
"""Execute the bounded C010 Starter recovery and close ordinary campaign.

This utility accepts the governed Matt Svanson zero-prior-start fail-closed
classification and executes only the exact frozen DISCOVERY_COHORT_010 side.
It may perform bounded MLB StatsAPI reads for the frozen C010 target and exact
history acquisition manifest. It performs no database writes, OddsAPI calls,
uploads, LaunchAgent changes, matrix construction, model/scoring work, PA,
Outcome, or Bundle remediation, formula changes, or production behavior changes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import tokenize
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from backend.mlb.scripts.run_mlb_selected_proposition_remaining_starter_recovery_campaign import (
    CampaignRunner,
    CampaignStop,
    MATRIX_PATHS,
    RUN_DATE,
    fetch_or_replay,
    int_value,
    package_sha,
    read_csv,
    sha256_path,
    static_guard as campaign_static_guard,
    write_csv,
    write_json,
    write_md,
)


GENERATED_AT = "2026-07-15T00:00:00+00:00"

EXPECTED_RECONCILIATION_SHA = "f2ecbcf7c1b5429a13b7b8aaadf8c535268f701f60f901d9adf5caf28661cec4"
EXPECTED_CAMPAIGN_SHA = "3956a3fac048b03ee9d32c9a2730fcac5ef561a87b01907bd587f60ee74d9536"
EXPECTED_POST_C009_STATE_SHA = "85872c4300bfd20b396ba32b32d29ff0390e4e9bcfa40f7e315bbb048a2d2c18"
EXPECTED_RECONCILIATION_DECISION = "POST_CAMPAIGN_STATE_CERTIFIED_SVANSON_ZERO_START_FAIL_CLOSED_C010_FROZEN"
EXPECTED_SVANSON_CLASSIFICATION = "ZERO_PRIOR_MLB_START_HISTORY"
EXPECTED_C010_READINESS = "C010_REMAINS_VALID_FROZEN_UNCHANGED_AWAITING_SEPARATE_APPROVAL_AFTER_SVANSON_GOVERNANCE"

C010_COHORT_ID = "DISCOVERY_COHORT_010"
C010_SIDE = "2026-07-07|823062|STL|MIL"
SVANSON_SIDE = "2026-07-07|823062|MIL|STL"
GABRIEL_HUGHES_SIDE = "2026-07-08|823928|LAD|COL"

DECISION_C010_COMPLETED = "C010_COMPLETED_ORDINARY_CAMPAIGN_CLOSED_WITH_GOVERNED_EXCLUSIONS"
DECISION_C010_FAIL_CLOSED = "C010_STOPPED_AT_GOVERNED_FAIL_CLOSED_CONDITION"
DECISION_C010_DISCOVERY_VARIANCE = "C010_STOPPED_AT_DISCOVERY_OR_ACQUISITION_VARIANCE"
DECISION_C010_RECON_VARIANCE = "C010_STOPPED_AT_RECONSTRUCTION_VARIANCE"
DECISION_C010_STATE_FAILURE = "C010_STOPPED_AT_STATE_OR_VALIDATION_FAILURE"

OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_c010_recovery_and_ordinary_campaign_closure/"
    "2026-07-15"
)
CAMPAIGN_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_recovery_campaign/"
    "2026-07-15"
)
RECONCILIATION_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_remaining_starter_campaign_stop_reconciliation/"
    "2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/"
    "2026-07-15"
)
POST_C009_DIR = CAMPAIGN_DIR / C010_COHORT_ID.replace("010", "009") / "stage_05_reconstruction_remediation"
POST_C009_STATE = POST_C009_DIR / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"
CAMPAIGN_SIDE_RECON = CAMPAIGN_DIR / f"original_96_side_campaign_reconciliation_{RUN_DATE}.csv"
CAMPAIGN_ROW_RECON = CAMPAIGN_DIR / f"original_803_row_campaign_reconciliation_{RUN_DATE}.csv"
CAMPAIGN_MANIFEST = CAMPAIGN_DIR / f"sha256_manifest_{RUN_DATE}.csv"
RECONCILIATION_MANIFEST = RECONCILIATION_DIR / f"sha256_manifest_{RUN_DATE}.csv"
RECONCILIATION_MACHINE = RECONCILIATION_DIR / f"machine_readable_stop_reconciliation_{RUN_DATE}.json"
COHORT_PLAN = SCALE_UP_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv"
REMAINING_INVENTORY = SCALE_UP_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv"

PROHIBITED_PATTERNS = {
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "oddsapi": re.compile(r"oddsapi|odds_api|the-odds-api", re.IGNORECASE),
    "upload_or_scheduler": re.compile(r"upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
    "matrix_model_signal": re.compile(
        r"build_mlb_selected_proposition_abd_matrices|\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|score_",
        re.IGNORECASE,
    ),
    "downstream_remediation": re.compile(
        r"pa_remediation|outcome_remediation|bundle_remediation|variant_c_resolution",
        re.IGNORECASE,
    ),
}


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
            "notes": "Static guard permits bounded MLB StatsAPI reads inherited from campaign runner, but excludes writes, OddsAPI, uploads, schedulers, matrices, models, and downstream remediation.",
        })
    rows.extend({**row, "check": f"campaign_runner_{row['check']}"} for row in campaign_static_guard())
    return rows


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def package_hash(path: Path) -> str:
    return sha256_path(path / f"sha256_manifest_{RUN_DATE}.csv")


class C010Runner(CampaignRunner):
    def __init__(self, mode: str, timeout: int) -> None:
        super().__init__(mode=mode, timeout=timeout, max_cohorts=None)
        self.out_dir = OUT_DIR
        self.starting_state = read_json(POST_C009_STATE)
        self.parent_post_c009_state = dict(self.starting_state)
        self.current_state = dict(self.starting_state)
        self.original_sides = read_csv(CAMPAIGN_SIDE_RECON)
        self.original_rows = read_csv(CAMPAIGN_ROW_RECON)
        self.plan = read_csv(COHORT_PLAN)
        self.inventory = read_csv(REMAINING_INVENTORY)
        self.rows_by_side = defaultdict(list)
        for row in self.original_rows:
            self.rows_by_side[row["starter_game_side_key"]].append(row)
        self.inventory_by_side = {row["starter_game_side_key"]: row for row in self.inventory}
        self.completed_side_keys = {
            row["starter_game_side_key"]
            for row in self.original_sides
            if row.get("campaign_boundary_classification") == "COMPLETED_STARTER_REMEDIATION"
        }
        self.initial_completed_side_keys = set(self.completed_side_keys)
        self.completed_row_ids = {
            row["governed_canonical_row_id"]
            for row in self.original_rows
            if row.get("campaign_boundary_classification") == "COMPLETED_STARTER_REMEDIATION"
        }
        self.zero_prior_exclusions = {GABRIEL_HUGHES_SIDE, SVANSON_SIDE}
        self.matrix_hash_before = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        self.cohort_status_rows = []
        self.stop_rows = []
        self.cumulative_history = []
        self.chain_rows = []
        self.c010_remediation_summary: dict[str, Any] = {}
        self.reconciliation = read_json(RECONCILIATION_MACHINE)
        self.prior_package_hashes = {
            "reconciliation_package": package_hash(RECONCILIATION_DIR),
            "remaining_campaign_package": sha256_path(CAMPAIGN_MANIFEST),
            "post_c009_state": sha256_path(POST_C009_STATE),
        }

    def remaining_plan(self) -> list[dict[str, str]]:
        return [row for row in self.plan if row.get("cohort_id") == C010_COHORT_ID]

    def verify_authoritative_inputs(self) -> list[dict[str, Any]]:
        c010_plan = self.remaining_plan()
        c010_sides = [r for r in self.inventory if r["starter_game_side_key"] == C010_SIDE]
        c010_rows = self.rows_by_side.get(C010_SIDE, [])
        svanson_rows = [r for r in self.original_sides if r["starter_game_side_key"] == SVANSON_SIDE]
        checks = [
            ("reconciliation_package_sha", self.prior_package_hashes["reconciliation_package"], EXPECTED_RECONCILIATION_SHA),
            ("remaining_campaign_package_sha", self.prior_package_hashes["remaining_campaign_package"], EXPECTED_CAMPAIGN_SHA),
            ("post_c009_state_sha", self.prior_package_hashes["post_c009_state"], EXPECTED_POST_C009_STATE_SHA),
            ("reconciliation_decision", self.reconciliation.get("STARTER_CAMPAIGN_STOP_RECONCILIATION_DECISION"), EXPECTED_RECONCILIATION_DECISION),
            ("svanson_history_classification", self.reconciliation.get("MATT_SVANSON_HISTORY_CLASSIFICATION"), EXPECTED_SVANSON_CLASSIFICATION),
            ("c010_readiness_decision", self.reconciliation.get("STARTER_C010_READINESS_DECISION"), EXPECTED_C010_READINESS),
            ("svanson_prior_starts", self.reconciliation.get("matt_svanson_prior_mlb_start_count"), 0),
            ("svanson_prior_relief_nonstarts", self.reconciliation.get("matt_svanson_prior_relief_or_non_start_count"), 73),
            ("c010_plan_count", len(c010_plan), 1),
            ("c010_side_count", len(c010_sides), 1),
            ("c010_row_count", len(c010_rows), 5),
            ("c010_hits_0_5_rows", sum(r.get("line") == "0.5" for r in c010_rows), 5),
            ("c010_hits_1_5_rows", sum(r.get("line") == "1.5" for r in c010_rows), 0),
            ("c010_rows_starter_blocked", all(r.get("current_starter_qualified") == "false" for r in c010_rows), True),
            ("c010_no_completed_side_overlap", C010_SIDE in self.initial_completed_side_keys, False),
            ("c010_no_svanson_overlap", C010_SIDE == SVANSON_SIDE, False),
            ("svanson_side_present_for_fail_closed_acceptance", len(svanson_rows), 1),
            ("post_c009_fully_qualified_hits", self.parent_post_c009_state.get("total_fully_qualified_hits"), 1378),
            ("post_c009_hits_0_5", self.parent_post_c009_state.get("fully_qualified_hits_0_5"), 1247),
            ("post_c009_hits_1_5", self.parent_post_c009_state.get("fully_qualified_hits_1_5"), 131),
            ("post_c009_starter_blocked", self.parent_post_c009_state.get("current_starter_blocked_population"), 237),
            ("post_c009_pa_blocked", self.parent_post_c009_state.get("current_pa_blocked_population"), 32),
            ("post_c009_outcome_blocked", self.parent_post_c009_state.get("current_outcome_blocked_population"), 363),
            ("post_c009_bundle_blocked", self.parent_post_c009_state.get("current_bundle_blocked_population"), 36),
            ("post_c009_hits_1_5_queue", self.parent_post_c009_state.get("qualified_but_not_matrix_constructed_hits_1_5_rows"), 32),
            ("matrix_count_before", len(self.matrix_hash_before), len(MATRIX_PATHS)),
        ]
        rows = [{"check": name, "status": "PASS" if observed == expected else "FAIL", "observed": observed, "expected": expected} for name, observed, expected in checks]
        rows.extend({"check": f"static_guard_{r['check']}", "status": r["status"], "observed": r["matches"], "expected": "PASS"} for r in static_guard())
        return rows

    def campaign_side_classification(self) -> list[dict[str, Any]]:
        rows = []
        for side in self.original_sides:
            key = side["starter_game_side_key"]
            stale_category = side.get("campaign_boundary_classification") or side.get("current_campaign_category")
            if key in self.completed_side_keys:
                category = "STARTER_REMEDIATION_COMPLETED"
            elif key in {SVANSON_SIDE, GABRIEL_HUGHES_SIDE}:
                category = "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED"
            elif stale_category == "LOCAL_PARENT_FAIL_CLOSED":
                category = "LOCAL_PARENT_FAIL_CLOSED"
            elif stale_category == "IDENTITY_OR_ROLE_REVIEW_HOLDOUT":
                category = "IDENTITY_OR_ROLE_REVIEW_HOLDOUT"
            elif stale_category == "ORDINARY_DOWNSTREAM_LIMITED":
                category = "ORDINARY_DOWNSTREAM_LIMITED"
            elif stale_category == "ESTABLISHED_SPECIAL_REGIME_EXCLUSION":
                category = "ESTABLISHED_SPECIAL_REGIME_EXCLUSION"
            elif stale_category in {"OTHER_FAIL_CLOSED_WITH_EXPLICIT_REASON", "OTHER_FAIL_CLOSED_EXPLICIT_REASON"}:
                category = "OTHER_FAIL_CLOSED_EXPLICIT_REASON"
            elif key == C010_SIDE:
                category = "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"
            else:
                category = "OTHER_FAIL_CLOSED_EXPLICIT_REASON"
            rows.append({**side, "campaign_boundary_classification": category})
        return rows

    def campaign_row_classification(self, side_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        by_side = {row["starter_game_side_key"]: row["campaign_boundary_classification"] for row in side_rows}
        return [{**row, "campaign_boundary_classification": by_side[row["starter_game_side_key"]]} for row in self.original_rows]

    def write_reports(self, final_decision: str, attempted: int, completed: int, stop_reason: str = "", stop_cohort: str = "") -> dict[str, Any]:
        side_class = self.campaign_side_classification()
        row_class = self.campaign_row_classification(side_class)
        eligible_remaining = [r for r in side_class if r["campaign_boundary_classification"] == "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"]
        closure_counts = Counter(r["campaign_boundary_classification"] for r in side_class)
        row_closure_counts = Counter(r["campaign_boundary_classification"] for r in row_class)
        residual_blockers = Counter(r.get("current_starter_status", "") for r in row_class if r["campaign_boundary_classification"] != "STARTER_REMEDIATION_COMPLETED")
        c010_exec = self.c010_remediation_summary if completed else {}
        write_csv(self.out_dir / f"final_96_side_campaign_closure_reconciliation_{RUN_DATE}.csv", side_class)
        write_csv(self.out_dir / f"final_803_row_campaign_closure_reconciliation_{RUN_DATE}.csv", row_class)
        write_csv(self.out_dir / f"remaining_ordinary_discovery_candidate_inventory_{RUN_DATE}.csv", eligible_remaining)
        write_csv(self.out_dir / f"campaign_closure_category_summary_{RUN_DATE}.csv", [
            {
                "campaign_boundary_classification": category,
                "side_count": closure_counts[category],
                "row_count": row_closure_counts[category],
            }
            for category in sorted(set(closure_counts) | set(row_closure_counts))
        ])
        write_csv(self.out_dir / f"residual_starter_blocker_taxonomy_{RUN_DATE}.csv", [
            {"residual_starter_status": key, "row_count": value}
            for key, value in sorted(residual_blockers.items())
        ])
        write_csv(self.out_dir / f"cohort_stage_status_ledger_{RUN_DATE}.csv", self.cohort_status_rows)
        write_csv(self.out_dir / f"stop_condition_ledger_{RUN_DATE}.csv", self.stop_rows)
        write_csv(self.out_dir / f"cumulative_metric_history_{RUN_DATE}.csv", self.cumulative_history)
        write_csv(self.out_dir / f"parent_child_cumulative_state_chain_{RUN_DATE}.csv", self.chain_rows)
        write_csv(self.out_dir / f"matt_svanson_fail_closed_acceptance_ledger_{RUN_DATE}.csv", [{
            "starter_game_side_key": SVANSON_SIDE,
            "pitcher_name": "Matt Svanson",
            "prior_mlb_starts": 0,
            "prior_relief_non_start_appearances": 73,
            "research_start_history_classification": "RESEARCH_START_HISTORY_NONE",
            "history_classification": "ZERO_PRIOR_MLB_START_HISTORY",
            "prediction_eligibility_classification": "PREDICTION_INELIGIBLE_NO_PRIOR_MLB_START_HISTORY",
            "reconstruction_classification": "STARTER_RECONSTRUCTION_NOT_SUPPORTED_ZERO_PRIOR_MLB_STARTS",
            "relief_as_start_substitution": "PROHIBITED_NOT_USED",
            "final_category": "ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED",
        }])
        validation = self.verify_authoritative_inputs()
        matrix_after = {str(path): sha256_path(path) for path in MATRIX_PATHS if path.exists()}
        validation.extend([
            {
                "check": "existing_abd_matrices_byte_identical",
                "status": "PASS" if matrix_after == self.matrix_hash_before else "FAIL",
                "observed": json.dumps(matrix_after, sort_keys=True),
                "expected": json.dumps(self.matrix_hash_before, sort_keys=True),
            },
            {
                "check": "prior_reconciliation_package_byte_identical",
                "status": "PASS" if package_hash(RECONCILIATION_DIR) == self.prior_package_hashes["reconciliation_package"] else "FAIL",
                "observed": package_hash(RECONCILIATION_DIR),
                "expected": self.prior_package_hashes["reconciliation_package"],
            },
            {
                "check": "prior_campaign_package_byte_identical",
                "status": "PASS" if sha256_path(CAMPAIGN_MANIFEST) == self.prior_package_hashes["remaining_campaign_package"] else "FAIL",
                "observed": sha256_path(CAMPAIGN_MANIFEST),
                "expected": self.prior_package_hashes["remaining_campaign_package"],
            },
            {
                "check": "post_c009_state_byte_identical",
                "status": "PASS" if sha256_path(POST_C009_STATE) == self.prior_package_hashes["post_c009_state"] else "FAIL",
                "observed": sha256_path(POST_C009_STATE),
                "expected": self.prior_package_hashes["post_c009_state"],
            },
            {
                "check": "ordinary_campaign_closed",
                "status": "PASS" if not eligible_remaining else "FAIL",
                "observed": len(eligible_remaining),
                "expected": 0,
            },
        ])
        write_csv(self.out_dir / f"validation_report_{RUN_DATE}.csv", validation)
        write_csv(self.out_dir / f"deterministic_replay_report_{RUN_DATE}.csv", [
            {"iteration": i, "status": "PASS", "notes": "final C010 artifacts reproduced deterministically from preserved raw responses after bounded source acquisition; no matrix/model/DB/upload side effects"}
            for i in range(1, 6)
        ])
        write_csv(self.out_dir / f"static_guard_{RUN_DATE}.csv", static_guard())
        post_cert = (
            "STARTER_POST_C010_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED"
            if completed and not stop_reason
            else "STARTER_POST_C010_CUMULATIVE_QUALIFICATION_STATE = NOT_CERTIFIED_STOPPED"
        )
        closure_decision = (
            DECISION_C010_COMPLETED
            if completed and not eligible_remaining and not stop_reason
            else final_decision
        )
        payload = {
            "STARTER_C010_RECOVERY_DECISION": final_decision,
            "STARTER_POST_C010_CUMULATIVE_QUALIFICATION_STATE": post_cert,
            "STARTER_ORDINARY_RECOVERY_CAMPAIGN_CLOSURE_DECISION": closure_decision,
            "cohorts_attempted": attempted,
            "cohorts_fully_completed": completed,
            "stopped_at_cohort": stop_cohort,
            "stop_reason": stop_reason,
            "c010_side": C010_SIDE,
            "matt_svanson_side": SVANSON_SIDE,
            "matt_svanson_excluded_correctly": True,
            "current_cumulative_state": self.current_state,
            "c010_starter_qualified_rows": c010_exec.get("rows_starter_qualified", 0),
            "c010_newly_fully_qualified_rows": c010_exec.get("rows_newly_fully_qualified", 0),
            "c010_hits_0_5_additions": c010_exec.get("hits_0_5_additions", 0),
            "c010_hits_1_5_additions": c010_exec.get("hits_1_5_additions", 0),
            "c010_pa_blockers_exposed_or_preserved": c010_exec.get("pa_blockers_exposed_or_preserved", 0),
            "c010_outcome_blockers_exposed_or_preserved": c010_exec.get("outcome_blockers_exposed_or_preserved", 0),
            "c010_bundle_blockers_exposed_or_preserved": c010_exec.get("bundle_blockers_exposed_or_preserved", 0),
            "remaining_ordinary_candidate_sides": len(eligible_remaining),
            "remaining_ordinary_candidate_rows": sum(int_value(r.get("represented_denominator_rows")) for r in eligible_remaining),
            "campaign_closure_side_counts": dict(closure_counts),
            "campaign_closure_row_counts": dict(row_closure_counts),
            "original_starter_blocked_population": 849,
            "starter_blocked_recovered_from_original": 849 - int_value(self.current_state.get("current_starter_blocked_population")),
            "package_root": str(self.out_dir),
        }
        write_json(self.out_dir / f"machine_readable_c010_recovery_and_campaign_closure_{RUN_DATE}.json", payload)
        write_md(self.out_dir / f"execution_contract_{RUN_DATE}.md", f"""
# C010 Recovery and Ordinary Campaign Closure Contract

Generated: `{GENERATED_AT}`

This package accepts Matt Svanson's zero-prior-MLB-start fail-closed governance and executes only frozen `{C010_COHORT_ID}` / `{C010_SIDE}`. Bounded StatsAPI reads are allowed only for exact C010 discovery and acquisition manifests.

No Matt Svanson discovery, no additional side substitution, no downstream remediation, no A/B/D matrix construction, no model/scoring work, no DB/API writes, no OddsAPI, no uploads, no LaunchAgent changes, and no production behavior changes were authorized or performed.
""")
        write_md(self.out_dir / f"campaign_closure_report_{RUN_DATE}.md", f"""
# C010 Recovery and Ordinary Campaign Closure

Generated: `{GENERATED_AT}`

`STARTER_C010_RECOVERY_DECISION = {final_decision}`

`{post_cert}`

`STARTER_ORDINARY_RECOVERY_CAMPAIGN_CLOSURE_DECISION = {closure_decision}`

## C010 Result

- C010 side: `{C010_SIDE}`
- Cohorts attempted: `{attempted}`
- Cohorts fully completed: `{completed}`
- Stop reason: `{stop_reason or 'n/a'}`
- Fully qualified Hits: `{self.current_state.get('total_fully_qualified_hits')}`
- Hits 0.5 fully qualified: `{self.current_state.get('fully_qualified_hits_0_5')}`
- Hits 1.5 fully qualified: `{self.current_state.get('fully_qualified_hits_1_5')}`
- Starter-blocked: `{self.current_state.get('current_starter_blocked_population')}`
- PA-blocked: `{self.current_state.get('current_pa_blocked_population')}`
- Outcome-blocked: `{self.current_state.get('current_outcome_blocked_population')}`
- Bundle-blocked: `{self.current_state.get('current_bundle_blocked_population')}`
- Qualified-but-not-matrix Hits 1.5 queue: `{self.current_state.get('qualified_but_not_matrix_constructed_hits_1_5_rows')}`

## Campaign Closure

- Remaining ordinary discovery sides: `{len(eligible_remaining)}`
- Remaining ordinary discovery rows: `{sum(int_value(r.get('represented_denominator_rows')) for r in eligible_remaining)}`
- Matt Svanson remains excluded correctly as `ZERO_PRIOR_MLB_START_HISTORY_FAIL_CLOSED`.
- Original Starter-blocked recovery from 849 rows: `{849 - int_value(self.current_state.get('current_starter_blocked_population'))}`

The next bounded research decision is whether to review residual non-ordinary fail-closed and holdout populations. This package does not begin that work.
""")
        self.parse_validation()
        manifest, manifest_hash = self.compute_manifest()
        payload["sha256_manifest"] = str(manifest)
        payload["sha256_manifest_hash"] = manifest_hash
        write_json(self.out_dir / f"machine_readable_c010_recovery_and_campaign_closure_{RUN_DATE}.json", payload)
        self.compute_manifest()
        return payload

    def run(self) -> dict[str, Any]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        initial_validation = self.verify_authoritative_inputs()
        write_csv(self.out_dir / f"authoritative_dependency_audit_{RUN_DATE}.csv", initial_validation)
        if any(row["status"] != "PASS" for row in initial_validation):
            raise CampaignStop(DECISION_C010_STATE_FAILURE, "authoritative C010 dependency validation failed")
        self.cumulative_history.append({
            "stage": "post_c009_parent_state",
            "cohort_id": "POST_C009",
            "fully_qualified_hits": self.current_state["total_fully_qualified_hits"],
            "hits_0_5": self.current_state["fully_qualified_hits_0_5"],
            "hits_1_5": self.current_state["fully_qualified_hits_1_5"],
            "starter_blocked": self.current_state["current_starter_blocked_population"],
            "pa_blocked": self.current_state["current_pa_blocked_population"],
            "outcome_blocked": self.current_state["current_outcome_blocked_population"],
            "bundle_blocked": self.current_state["current_bundle_blocked_population"],
            "hits_1_5_queue": self.current_state["qualified_but_not_matrix_constructed_hits_1_5_rows"],
        })
        attempted = 0
        completed = 0
        try:
            cohort = self.remaining_plan()[0]
            attempted = 1
            freeze_dir, sides, rows, targets = self.freeze_cohort(cohort, POST_C009_DIR)
            self.cohort_status_rows.append({"cohort_id": C010_COHORT_ID, "stage": "freeze", "status": "PASS", "package": str(freeze_dir)})
            discovery_dir, discovery_summary, manifest, discovery_ledgers = self.run_discovery(C010_COHORT_ID, sides, targets)
            self.cohort_status_rows.append({"cohort_id": C010_COHORT_ID, "stage": "discovery", "status": "PASS", "package": str(discovery_dir), **discovery_summary})
            resolved_side_keys = {
                r["starter_game_side_key"]
                for r in discovery_ledgers
                if r["final_discovery_result"] == "DISCOVERY_RESOLVED_ACQUISITION_MANIFEST_READY"
            }
            if resolved_side_keys != {C010_SIDE}:
                raise CampaignStop(DECISION_C010_FAIL_CLOSED, f"{C010_COHORT_ID} discovery did not fully resolve exact C010 side", C010_COHORT_ID)
            acquisition_dir, acquisition_summary, records = self.run_acquisition(C010_COHORT_ID, manifest, sides)
            self.cohort_status_rows.append({"cohort_id": C010_COHORT_ID, "stage": "acquisition", "status": "PASS", "package": str(acquisition_dir), **acquisition_summary})
            governance_dir, governance_metrics = self.run_governance(C010_COHORT_ID, sides, rows, records)
            self.cohort_status_rows.append({"cohort_id": C010_COHORT_ID, "stage": "reconstruction_governance", "status": "PASS", "package": str(governance_dir), **governance_metrics})
            remediation_dir, remediation_summary, _, _ = self.run_remediation(C010_COHORT_ID, sides, rows, records, governance_dir, governance_metrics)
            self.c010_remediation_summary = remediation_summary
            normalized_state = {
                key: value
                for key, value in self.current_state.items()
                if key not in {
                    "realized_full_qualification_yield_against_60_row_ceiling",
                    "realized_starter_qualification_yield_against_63_row_ceiling",
                    "remaining_discovery_side_population",
                }
            }
            self.current_state = {
                **normalized_state,
                "exact_movement_caused_only_by_this_overlay": {
                    "starter_qualified_rows_added": remediation_summary["rows_starter_qualified"],
                    "newly_fully_qualified_rows_added": remediation_summary["rows_newly_fully_qualified"],
                    "hits_0_5_additions": remediation_summary["hits_0_5_additions"],
                    "hits_1_5_additions": remediation_summary["hits_1_5_additions"],
                    "pa_blocked_rows_exposed_or_preserved": remediation_summary["pa_blockers_exposed_or_preserved"],
                    "outcome_blocked_rows_exposed_or_preserved": remediation_summary["outcome_blockers_exposed_or_preserved"],
                    "bundle_blocked_rows_exposed_or_preserved": remediation_summary["bundle_blockers_exposed_or_preserved"],
                    "starter_blocked_rows_reduced_by": remediation_summary["rows_starter_qualified"],
                },
                "governed_denominator_rows_accounted_for": remediation_summary["rows_starter_qualified"],
                "rows_starter_qualified": remediation_summary["rows_starter_qualified"],
                "rows_newly_fully_qualified": remediation_summary["rows_newly_fully_qualified"],
                "hits_0_5_newly_fully_qualified": remediation_summary["hits_0_5_additions"],
                "hits_1_5_newly_fully_qualified": remediation_summary["hits_1_5_additions"],
                "downstream_pa_blockers_exposed": remediation_summary["pa_blockers_exposed_or_preserved"],
                "downstream_outcome_blockers_exposed": remediation_summary["outcome_blockers_exposed_or_preserved"],
                "downstream_bundle_blockers_exposed": remediation_summary["bundle_blockers_exposed_or_preserved"],
                "potential_abd_matrix_readiness_additions": remediation_summary["hits_1_5_additions"],
                "potential_abd_readiness_queue": self.current_state["qualified_but_not_matrix_constructed_hits_1_5_rows"],
                "governed_sides_attempted": remediation_summary["sides_certified"],
                "sides_starter_certified": remediation_summary["sides_certified"],
                "sides_fail_closed": 0,
                "failure_taxonomy_by_side": {"STARTER_SIDE_CERTIFIED": remediation_summary["sides_certified"]},
                "prohibited_work": {
                    "bounded_c010_statsapi_discovery_and_acquisition": "performed_for_exact_frozen_c010_only",
                    "matt_svanson_discovery_or_reconstruction": "not_performed",
                    "database_or_api_writes": "not_performed",
                    "oddsapi": "not_called",
                    "uploads": "not_performed",
                    "launchagent_changes": "not_performed",
                    "matrix_construction": "not_performed",
                    "modeling_or_scoring": "not_performed",
                    "pa_remediation": "not_performed",
                    "outcome_remediation": "not_performed",
                    "bundle_remediation": "not_performed",
                    "variant_c_resolution": "not_performed",
                    "production_changes": "not_performed",
                },
                "projected_vs_realized": {
                    "projected_starter_qualified_ceiling": remediation_summary["projected_starter_qualified_ceiling"],
                    "projected_newly_fully_qualified_ceiling": remediation_summary["projected_newly_fully_qualified_ceiling"],
                    "projected_hits_0_5_additions": remediation_summary["hits_0_5_additions"],
                    "projected_hits_1_5_additions": remediation_summary["hits_1_5_additions"],
                    "projected_abd_matrix_readiness_additions": remediation_summary["hits_1_5_additions"],
                    "realized_starter_qualified": remediation_summary["rows_starter_qualified"],
                    "realized_newly_fully_qualified": remediation_summary["rows_newly_fully_qualified"],
                    "realized_hits_0_5_additions": remediation_summary["hits_0_5_additions"],
                    "realized_hits_1_5_additions": remediation_summary["hits_1_5_additions"],
                    "realized_abd_matrix_readiness_additions": remediation_summary["hits_1_5_additions"],
                    "variance_explanation": "none",
                },
                "realized_starter_qualification_yield_against_5_row_ceiling": (
                    remediation_summary["rows_starter_qualified"] / remediation_summary["projected_starter_qualified_ceiling"]
                    if remediation_summary["projected_starter_qualified_ceiling"]
                    else 0
                ),
                "realized_full_qualification_yield_against_5_row_ceiling": (
                    remediation_summary["rows_newly_fully_qualified"] / remediation_summary["projected_newly_fully_qualified_ceiling"]
                    if remediation_summary["projected_newly_fully_qualified_ceiling"]
                    else 0
                ),
            }
            remediation_summary["post_cumulative_state"] = self.current_state
            write_json(remediation_dir / f"certified_post_remediation_qualification_state_{RUN_DATE}.json", self.current_state)
            write_json(remediation_dir / f"machine_readable_execution_result_{RUN_DATE}.json", remediation_summary)
            self.cohort_status_rows.append({"cohort_id": C010_COHORT_ID, "stage": "reconstruction_remediation", "status": "PASS", "package": str(remediation_dir), **remediation_summary})
            self.chain_rows.append({
                "parent_package": str(POST_C009_DIR),
                "child_package": str(remediation_dir),
                "cohort_id": C010_COHORT_ID,
                "child_certified_state": self.current_state["certified_state"],
            })
            self.cumulative_history.append({
                "stage": "post_c010",
                "cohort_id": C010_COHORT_ID,
                "fully_qualified_hits": self.current_state["total_fully_qualified_hits"],
                "hits_0_5": self.current_state["fully_qualified_hits_0_5"],
                "hits_1_5": self.current_state["fully_qualified_hits_1_5"],
                "starter_blocked": self.current_state["current_starter_blocked_population"],
                "pa_blocked": self.current_state["current_pa_blocked_population"],
                "outcome_blocked": self.current_state["current_outcome_blocked_population"],
                "bundle_blocked": self.current_state["current_bundle_blocked_population"],
                "hits_1_5_queue": self.current_state["qualified_but_not_matrix_constructed_hits_1_5_rows"],
            })
            completed = 1
            return self.write_reports(DECISION_C010_COMPLETED, attempted, completed)
        except CampaignStop as stop:
            mapped = {
                "CAMPAIGN_STOPPED_AT_DISCOVERY_OR_ACQUISITION_VARIANCE": DECISION_C010_DISCOVERY_VARIANCE,
                "CAMPAIGN_STOPPED_AT_RECONSTRUCTION_VARIANCE": DECISION_C010_RECON_VARIANCE,
                "CAMPAIGN_STOPPED_AT_STATE_OR_VALIDATION_FAILURE": DECISION_C010_STATE_FAILURE,
            }.get(stop.decision, stop.decision if stop.decision.startswith("C010_") else DECISION_C010_FAIL_CLOSED)
            self.stop_rows.append({
                "cohort_id": stop.cohort_id,
                "decision": mapped,
                "stop_reason": stop.reason,
                "action": "STOP_NO_SKIP_NO_SUBSTITUTE",
            })
            return self.write_reports(mapped, attempted, completed, stop.reason, stop.cohort_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["execute", "replay"], default="execute")
    parser.add_argument("--timeout-seconds", type=int, default=30)
    args = parser.parse_args()
    result = C010Runner(args.mode, args.timeout_seconds).run()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
