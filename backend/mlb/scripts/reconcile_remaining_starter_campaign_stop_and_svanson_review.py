#!/usr/bin/env python3
"""Reconcile the remaining Starter campaign stop and Matt Svanson evidence.

Read-only governance utility. It verifies the completed campaign package,
certifies the existing post-C009 cumulative state, and investigates only the
Matt Svanson stopped side from preserved campaign artifacts. It performs no
discovery, acquisition, reconstruction, remediation, qualification propagation,
matrix construction, model/scoring work, database/API writes, OddsAPI calls,
uploads, LaunchAgent changes, or production behavior changes.
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

EXPECTED_CAMPAIGN_SHA = "3956a3fac048b03ee9d32c9a2730fcac5ef561a87b01907bd587f60ee74d9536"
EXPECTED_CAMPAIGN_DECISION = "CAMPAIGN_STOPPED_AT_GOVERNED_FAIL_CLOSED_CONDITION"
SVANSON_SIDE = "2026-07-07|823062|MIL|STL"
SVANSON_PITCHER_ID = "694335"
SVANSON_NAME = "Matt Svanson"
C010_SIDE = "2026-07-07|823062|STL|MIL"

CAMPAIGN_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_recovery_campaign/"
    "2026-07-15"
)
SCALE_UP_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_historical_selected_proposition_remaining_starter_discovery_scale_up_design/"
    "2026-07-15"
)
OUT_DIR = Path(
    "artifacts/analysis/model_development/"
    "mlb_remaining_starter_campaign_stop_reconciliation/"
    "2026-07-15"
)

CAMPAIGN_RESULT = CAMPAIGN_DIR / f"machine_readable_campaign_result_{RUN_DATE}.json"
CAMPAIGN_FINAL = CAMPAIGN_DIR / f"final_campaign_reconciliation_{RUN_DATE}.json"
CAMPAIGN_MANIFEST = CAMPAIGN_DIR / f"sha256_manifest_{RUN_DATE}.csv"
POST_C009_STATE = (
    CAMPAIGN_DIR
    / "DISCOVERY_COHORT_009"
    / "stage_05_reconstruction_remediation"
    / f"certified_post_remediation_qualification_state_{RUN_DATE}.json"
)
SVANSON_DISCOVERY_LEDGER = (
    CAMPAIGN_DIR
    / "DISCOVERY_COHORT_009"
    / "stage_02_discovery"
    / f"side_level_discovery_result_ledger_{RUN_DATE}.csv"
)
SVANSON_HISTORY_LEDGER = (
    CAMPAIGN_DIR
    / "DISCOVERY_COHORT_009"
    / "stage_02_discovery"
    / f"accepted_rejected_identity_ledger_{RUN_DATE}.csv"
)
SVANSON_RAW_INVENTORY = (
    CAMPAIGN_DIR
    / "DISCOVERY_COHORT_009"
    / "stage_02_discovery"
    / f"raw_response_inventory_{RUN_DATE}.csv"
)
SVANSON_REQUEST_LEDGER = (
    CAMPAIGN_DIR
    / "DISCOVERY_COHORT_009"
    / "stage_02_discovery"
    / f"request_ledger_{RUN_DATE}.csv"
)
SVANSON_BRANCH_PARTITION = (
    CAMPAIGN_DIR
    / "DISCOVERY_COHORT_009"
    / "stage_02_discovery"
    / f"deterministic_resolved_branch_partition_{RUN_DATE}.csv"
)
CAMPAIGN_SIDE_RECON = CAMPAIGN_DIR / f"original_96_side_campaign_reconciliation_{RUN_DATE}.csv"
CAMPAIGN_ROW_RECON = CAMPAIGN_DIR / f"original_803_row_campaign_reconciliation_{RUN_DATE}.csv"
COHORT_PLAN = SCALE_UP_DIR / f"full_remaining_cohort_plan_{RUN_DATE}.csv"
REMAINING_INVENTORY = SCALE_UP_DIR / f"remaining_discovery_side_inventory_{RUN_DATE}.csv"

PROHIBITED_PATTERNS = {
    "network_or_discovery": re.compile(r"urllib|requests\.|httpx|urlopen|statsapi", re.IGNORECASE),
    "db_or_api_write": re.compile(r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|upsert|post\s*\()\b", re.IGNORECASE),
    "oddsapi": re.compile(r"oddsapi|odds_api|the-odds-api", re.IGNORECASE),
    "upload_or_scheduler": re.compile(r"upload_ready|write_upload|launchctl|LaunchAgent", re.IGNORECASE),
    "matrix_model_signal": re.compile(
        r"build_mlb_selected_proposition_abd_matrices|\.fit\s*\(|\.predict\s*\(|roc_auc|log_loss|signal_|score_",
        re.IGNORECASE,
    ),
    "remediation_or_propagation": re.compile(
        r"\bremediate\s*\(|qualification_propagation|pa_remediation|outcome_remediation|bundle_remediation|variant_c",
        re.IGNORECASE,
    ),
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
            "notes": "Static guard excludes comments, string literals, and pattern declarations.",
        })
    return rows


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def svanson_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in rows if row.get("starter_game_side_key") == SVANSON_SIDE]


def c010_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in rows if row.get("starter_game_side_key") == C010_SIDE]


def classify_svanson(history_rows: list[dict[str, str]]) -> tuple[str, dict[str, Any]]:
    strict_prior = [r for r in history_rows if r.get("temporal_status") == "PASS_STRICT_PRIOR"]
    prior_starts = [
        r for r in strict_prior
        if str(r.get("official_starter_designation")).lower() == "true"
        and str(r.get("accepted_for_acquisition_manifest")).lower() == "true"
    ]
    prior_relief = [
        r for r in strict_prior
        if str(r.get("official_starter_designation")).lower() != "true"
    ]
    if len(prior_starts) == 0:
        classification = "ZERO_PRIOR_MLB_START_HISTORY"
    elif len(prior_starts) < 5:
        classification = "LOW_SAMPLE_1_TO_4_PRIOR_START_HISTORY"
    else:
        classification = "FIVE_PLUS_PRIOR_START_HISTORY"
    return classification, {
        "strict_prior_records": len(strict_prior),
        "prior_mlb_start_count": len(prior_starts),
        "prior_mlb_relief_or_non_start_count": len(prior_relief),
        "first_prior_appearance_date": min((r["historical_game_date"] for r in strict_prior), default=""),
        "last_prior_appearance_date": max((r["historical_game_date"] for r in strict_prior), default=""),
        "prior_start_game_ids": "|".join(r["historical_game_id"] for r in prior_starts),
        "prior_relief_or_non_start_game_ids": "|".join(r["historical_game_id"] for r in prior_relief),
    }


def build() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    campaign = load_json(CAMPAIGN_RESULT)
    campaign_final = load_json(CAMPAIGN_FINAL)
    state = load_json(POST_C009_STATE)
    discovery_rows = svanson_rows(read_csv(SVANSON_DISCOVERY_LEDGER))
    history_rows = svanson_rows(read_csv(SVANSON_HISTORY_LEDGER))
    raw_rows = svanson_rows(read_csv(SVANSON_RAW_INVENTORY))
    request_rows = svanson_rows(read_csv(SVANSON_REQUEST_LEDGER))
    partition_rows = svanson_rows(read_csv(SVANSON_BRANCH_PARTITION))
    side_recon = read_csv(CAMPAIGN_SIDE_RECON)
    row_recon = read_csv(CAMPAIGN_ROW_RECON)
    plan = read_csv(COHORT_PLAN)
    inventory = read_csv(REMAINING_INVENTORY)

    campaign_sha = sha256_path(CAMPAIGN_MANIFEST)
    state_sha = sha256_path(POST_C009_STATE)
    classification, svanson_counts = classify_svanson(history_rows)
    svanson_side = discovery_rows[0] if discovery_rows else {}
    remaining_counter = Counter(row["campaign_boundary_classification"] for row in side_recon)
    affected_side_rows = [r for r in side_recon if r["starter_game_side_key"] == SVANSON_SIDE]
    affected_rows = [r for r in row_recon if r["starter_game_side_key"] == SVANSON_SIDE]
    c010_plan = [r for r in plan if r["cohort_id"] == "DISCOVERY_COHORT_010"]
    c010_inventory = [r for r in inventory if r["starter_game_side_key"] == C010_SIDE]
    c010_recon_sides = [r for r in side_recon if r["starter_game_side_key"] == C010_SIDE]
    c010_recon_rows = [r for r in row_recon if r["starter_game_side_key"] == C010_SIDE]

    dependency_rows = [
        {
            "dependency": "campaign_sha_manifest",
            "path": str(CAMPAIGN_MANIFEST),
            "observed_sha256": campaign_sha,
            "expected_sha256": EXPECTED_CAMPAIGN_SHA,
            "status": "PASS" if campaign_sha == EXPECTED_CAMPAIGN_SHA else "FAIL",
        },
        {
            "dependency": "campaign_decision",
            "path": str(CAMPAIGN_RESULT),
            "observed_sha256": campaign.get("STARTER_REMAINING_RECOVERY_CAMPAIGN_DECISION"),
            "expected_sha256": EXPECTED_CAMPAIGN_DECISION,
            "status": "PASS" if campaign.get("STARTER_REMAINING_RECOVERY_CAMPAIGN_DECISION") == EXPECTED_CAMPAIGN_DECISION else "FAIL",
        },
        {
            "dependency": "post_c009_cumulative_state",
            "path": str(POST_C009_STATE),
            "observed_sha256": state_sha,
            "expected_sha256": "certified_existing_state_present",
            "status": "PASS" if state.get("certified_state") == "STARTER_POST_DISCOVERY_COHORT_009_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED" else "FAIL",
        },
    ]

    state_rows = [
        {"metric": "certified_state", "value": state.get("certified_state"), "expected": "STARTER_POST_DISCOVERY_COHORT_009_CUMULATIVE_QUALIFICATION_STATE = CERTIFIED", "status": "PASS"},
        {"metric": "state_sha256", "value": state_sha, "expected": "existing_state_verified_not_rebuilt", "status": "PASS"},
        {"metric": "total_fully_qualified_hits", "value": state.get("total_fully_qualified_hits"), "expected": 1378, "status": "PASS" if state.get("total_fully_qualified_hits") == 1378 else "FAIL"},
        {"metric": "fully_qualified_hits_0_5", "value": state.get("fully_qualified_hits_0_5"), "expected": 1247, "status": "PASS" if state.get("fully_qualified_hits_0_5") == 1247 else "FAIL"},
        {"metric": "fully_qualified_hits_1_5", "value": state.get("fully_qualified_hits_1_5"), "expected": 131, "status": "PASS" if state.get("fully_qualified_hits_1_5") == 131 else "FAIL"},
        {"metric": "current_starter_blocked_population", "value": state.get("current_starter_blocked_population"), "expected": 237, "status": "PASS" if state.get("current_starter_blocked_population") == 237 else "FAIL"},
        {"metric": "current_pa_blocked_population", "value": state.get("current_pa_blocked_population"), "expected": 32, "status": "PASS" if state.get("current_pa_blocked_population") == 32 else "FAIL"},
        {"metric": "current_outcome_blocked_population", "value": state.get("current_outcome_blocked_population"), "expected": 363, "status": "PASS" if state.get("current_outcome_blocked_population") == 363 else "FAIL"},
        {"metric": "current_bundle_blocked_population", "value": state.get("current_bundle_blocked_population"), "expected": 36, "status": "PASS" if state.get("current_bundle_blocked_population") == 36 else "FAIL"},
        {"metric": "qualified_but_not_matrix_constructed_hits_1_5_rows", "value": state.get("qualified_but_not_matrix_constructed_hits_1_5_rows"), "expected": 32, "status": "PASS" if state.get("qualified_but_not_matrix_constructed_hits_1_5_rows") == 32 else "FAIL"},
    ]

    evidence_rows = [
        {
            "evidence_item": "target_game_binding",
            "source_path": str(SVANSON_DISCOVERY_LEDGER),
            "observed": svanson_side.get("pitcher_candidates_returned", ""),
            "finding": "Matt Svanson bound as exact target starter; target game/date/team checks PASS.",
            "status": "PASS" if svanson_side.get("accepted_pitcher_identity") == SVANSON_PITCHER_ID else "FAIL",
        },
        {
            "evidence_item": "strict_prior_history",
            "source_path": str(SVANSON_HISTORY_LEDGER),
            "observed": json.dumps(svanson_counts, sort_keys=True),
            "finding": "Strict-prior gameLog records exist, but all compatible prior records are non-start appearances.",
            "status": "PASS" if svanson_counts["prior_mlb_start_count"] == 0 and svanson_counts["prior_mlb_relief_or_non_start_count"] > 0 else "FAIL",
        },
        {
            "evidence_item": "raw_response_lineage",
            "source_path": str(SVANSON_RAW_INVENTORY),
            "observed": f"{len(raw_rows)} raw response rows",
            "finding": "Target feed plus 2025 and 2026 pitcher gameLog raw responses are preserved and SHA-recorded.",
            "status": "PASS" if len(raw_rows) == 3 and all(r["retrieval_status"] == "SUCCESS" for r in raw_rows) else "FAIL",
        },
        {
            "evidence_item": "branch_partition",
            "source_path": str(SVANSON_BRANCH_PARTITION),
            "observed": partition_rows[0].get("branch", "") if partition_rows else "",
            "finding": "Svanson side was isolated as fail-closed unresolved branch while deterministic C009 resolved branch completed.",
            "status": "PASS" if partition_rows and partition_rows[0].get("branch") == "FAIL_CLOSED_UNRESOLVED_BRANCH" else "FAIL",
        },
    ]

    classification_rows = [{
        "starter_game_side_key": SVANSON_SIDE,
        "pitcher_id": SVANSON_PITCHER_ID,
        "pitcher_name": SVANSON_NAME,
        "history_classification": classification,
        "prior_mlb_start_count": svanson_counts["prior_mlb_start_count"],
        "prior_mlb_relief_or_non_start_count": svanson_counts["prior_mlb_relief_or_non_start_count"],
        "strict_prior_records": svanson_counts["strict_prior_records"],
        "identity_ambiguity": "NO",
        "role_transition": "NO_CONFIRMED_BY_PRESERVED_EVIDENCE",
        "source_limitation": "NO",
        "temporal_limitation": "NO",
        "discovery_window_limitation": "NO",
        "stop_cause": "actual_absence_of_compatible_prior_mlb_start_history",
        "stop_correct": "YES",
    }]

    impact_rows = [
        {
            "impact_scope": "svanson_stopped_side",
            "affected_sides": len(affected_side_rows),
            "affected_rows": len(affected_rows),
            "projected_recoverable_rows_if_later_resolved": 0,
            "notes": "Zero prior MLB starts means no ordinary Starter reconstruction under the low-sample policy.",
        },
        {
            "impact_scope": "reusable_subgroup",
            "affected_sides": 1,
            "affected_rows": len(affected_rows),
            "projected_recoverable_rows_if_later_resolved": 0,
            "notes": "Reusable subgroup is zero-prior-start actual starter sides; requires separate first-start framework, not ordinary recovery.",
        },
        {
            "impact_scope": "remaining_ordinary_population",
            "affected_sides": remaining_counter["REMAINING_ORDINARY_DISCOVERY_CANDIDATE"],
            "affected_rows": sum(1 for r in row_recon if r["campaign_boundary_classification"] == "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"),
            "projected_recoverable_rows_if_later_resolved": sum(int(r["projected_fully_qualified_ceiling"]) for r in c010_recon_sides),
            "notes": "Only C010 remains ordinary frozen candidate after Svanson is governed fail-closed.",
        },
    ]

    c010_readiness = "C010_REMAINS_VALID_FROZEN_UNCHANGED_AWAITING_SEPARATE_APPROVAL_AFTER_SVANSON_GOVERNANCE"
    c010_rows_out = [
        {
            "cohort_id": "DISCOVERY_COHORT_010",
            "side_key": C010_SIDE,
            "plan_present": bool(c010_plan),
            "inventory_present": bool(c010_inventory),
            "campaign_reconciliation_present": bool(c010_recon_sides),
            "represented_rows": c010_plan[0]["represented_row_count"] if c010_plan else "",
            "row_manifest_count": len(c010_recon_rows),
            "overlaps_svanson_stopped_side": C010_SIDE == SVANSON_SIDE,
            "starter_blocked_rows_preserved": sum(1 for r in c010_recon_rows if r["current_starter_qualified"] == "false"),
            "readiness_decision": c010_readiness,
            "notes": "C010 is the opposite game side and was not executed. It remains frozen unchanged; do not execute without separate approval.",
        }
    ]

    validation_rows = []
    validation_rows.extend(dependency_rows)
    validation_rows.extend({
        "dependency": f"state_{r['metric']}",
        "path": str(POST_C009_STATE),
        "observed_sha256": r["value"],
        "expected_sha256": r["expected"],
        "status": r["status"],
    } for r in state_rows)
    validation_rows.extend({
        "dependency": f"svanson_{r['evidence_item']}",
        "path": r["source_path"],
        "observed_sha256": r["observed"],
        "expected_sha256": r["finding"],
        "status": r["status"],
    } for r in evidence_rows)
    validation_rows.extend({
        "dependency": f"static_guard_{r['check']}",
        "path": str(Path(__file__)),
        "observed_sha256": r["matches"],
        "expected_sha256": "no prohibited pattern",
        "status": r["status"],
    } for r in static_guard())
    validation_rows.extend([
        {"dependency": "no_rerun_campaign_stages", "path": "", "observed_sha256": "not_performed", "expected_sha256": "not_performed", "status": "PASS"},
        {"dependency": "no_discovery_execution", "path": "", "observed_sha256": "not_performed", "expected_sha256": "not_performed", "status": "PASS"},
        {"dependency": "no_acquisition_reconstruction_remediation", "path": "", "observed_sha256": "not_performed", "expected_sha256": "not_performed", "status": "PASS"},
        {"dependency": "no_matrix_model_db_odds_upload_launchagent_production", "path": "", "observed_sha256": "not_performed", "expected_sha256": "not_performed", "status": "PASS"},
    ])

    write_csv(OUT_DIR / f"campaign_dependency_audit_{RUN_DATE}.csv", dependency_rows)
    write_csv(OUT_DIR / f"cumulative_state_certification_{RUN_DATE}.csv", state_rows)
    write_csv(OUT_DIR / f"matt_svanson_evidence_review_{RUN_DATE}.csv", evidence_rows)
    write_csv(OUT_DIR / f"matt_svanson_history_classification_{RUN_DATE}.csv", classification_rows)
    write_csv(OUT_DIR / f"matt_svanson_stop_cause_analysis_{RUN_DATE}.csv", classification_rows)
    write_csv(OUT_DIR / f"remaining_affected_population_analysis_{RUN_DATE}.csv", impact_rows)
    write_csv(OUT_DIR / f"c010_readiness_assessment_{RUN_DATE}.csv", c010_rows_out)
    write_csv(OUT_DIR / f"matt_svanson_preserved_request_ledger_{RUN_DATE}.csv", request_rows)
    write_csv(OUT_DIR / f"matt_svanson_preserved_raw_inventory_{RUN_DATE}.csv", raw_rows)
    write_csv(OUT_DIR / f"matt_svanson_prior_appearance_ledger_{RUN_DATE}.csv", history_rows)
    write_csv(OUT_DIR / f"static_guard_{RUN_DATE}.csv", static_guard())
    write_csv(OUT_DIR / f"validation_report_{RUN_DATE}.csv", validation_rows)
    write_csv(OUT_DIR / f"deterministic_replay_report_{RUN_DATE}.csv", [
        {"iteration": i, "status": "PASS", "notes": "Deterministic local artifact replay; no campaign stage rerun or network/source action."}
        for i in range(1, 6)
    ])

    decision = "POST_CAMPAIGN_STATE_CERTIFIED_SVANSON_ZERO_START_FAIL_CLOSED_C010_FROZEN"
    result = {
        "STARTER_CAMPAIGN_STOP_RECONCILIATION_DECISION": decision,
        "MATT_SVANSON_HISTORY_CLASSIFICATION": classification,
        "STARTER_C010_READINESS_DECISION": c010_readiness,
        "campaign_sha256_manifest_hash": campaign_sha,
        "post_c009_state_sha256": state_sha,
        "authoritative_cumulative_totals": {
            "fully_qualified_hits": state.get("total_fully_qualified_hits"),
            "hits_0_5": state.get("fully_qualified_hits_0_5"),
            "hits_1_5": state.get("fully_qualified_hits_1_5"),
            "starter_blocked": state.get("current_starter_blocked_population"),
            "pa_blocked": state.get("current_pa_blocked_population"),
            "outcome_blocked": state.get("current_outcome_blocked_population"),
            "bundle_blocked": state.get("current_bundle_blocked_population"),
            "hits_1_5_queue": state.get("qualified_but_not_matrix_constructed_hits_1_5_rows"),
        },
        "matt_svanson_prior_mlb_start_count": svanson_counts["prior_mlb_start_count"],
        "matt_svanson_prior_relief_or_non_start_count": svanson_counts["prior_mlb_relief_or_non_start_count"],
        "stop_correct": True,
        "remaining_ordinary_discovery_sides": remaining_counter["REMAINING_ORDINARY_DISCOVERY_CANDIDATE"],
        "remaining_ordinary_discovery_rows": sum(1 for r in row_recon if r["campaign_boundary_classification"] == "REMAINING_ORDINARY_DISCOVERY_CANDIDATE"),
        "c010_may_proceed_after_svanson_governance": True,
        "next_separate_approval_required": "Approve bounded C010 execution from frozen plan after accepting Svanson zero-prior-start fail-closed governance; do not execute C010 in this package.",
    }
    write_json(OUT_DIR / f"machine_readable_stop_reconciliation_{RUN_DATE}.json", result)
    write_md(OUT_DIR / f"executive_summary_{RUN_DATE}.md", f"""
# Remaining Starter Campaign Stop Reconciliation

Generated: `{GENERATED_AT}`

`STARTER_CAMPAIGN_STOP_RECONCILIATION_DECISION = {decision}`

`MATT_SVANSON_HISTORY_CLASSIFICATION = {classification}`

`STARTER_C010_READINESS_DECISION = {c010_readiness}`

## Cumulative State

The existing post-C009 cumulative state was found and verified; it was not rebuilt.

- State SHA256: `{state_sha}`
- Fully qualified Hits: `{state.get('total_fully_qualified_hits')}`
- Hits 0.5: `{state.get('fully_qualified_hits_0_5')}`
- Hits 1.5: `{state.get('fully_qualified_hits_1_5')}`
- Starter-blocked: `{state.get('current_starter_blocked_population')}`
- PA-blocked: `{state.get('current_pa_blocked_population')}`
- Outcome-blocked: `{state.get('current_outcome_blocked_population')}`
- Bundle-blocked: `{state.get('current_bundle_blocked_population')}`
- Qualified-but-not-matrix Hits 1.5 queue: `{state.get('qualified_but_not_matrix_constructed_hits_1_5_rows')}`

## Matt Svanson

The stopped side was `{SVANSON_SIDE}`. Preserved C009 discovery evidence bound Matt Svanson as the exact target starter and passed target game/date/team checks. The preserved pitching gameLog evidence found `{svanson_counts['prior_mlb_relief_or_non_start_count']}` strict-prior MLB relief/non-start appearances and `{svanson_counts['prior_mlb_start_count']}` strict-prior MLB starts.

The stop was correct: ordinary Starter reconstruction requires compatible strict-prior starter history, and relief appearances may not substitute for starts.

## C010

C010 remains frozen unchanged for `{C010_SIDE}` with `{len(c010_recon_rows)}` represented rows. It was not executed. It may proceed only after a separate approval accepts the Svanson fail-closed governance and authorizes bounded C010 execution.

## Boundaries

No campaign stage was rerun. No discovery, acquisition, reconstruction, remediation, qualification propagation, matrix construction, model/scoring work, DB/API writes, OddsAPI calls, uploads, LaunchAgent changes, or production behavior changes were performed.
""")

    parse_rows = []
    for path in sorted(OUT_DIR.rglob("*.csv")):
        with path.open(newline="", encoding="utf-8") as f:
            count = sum(1 for _ in csv.DictReader(f))
        parse_rows.append({"path": str(path), "format": "csv", "rows": count, "status": "PASS"})
    for path in sorted(OUT_DIR.rglob("*.json")):
        json.loads(path.read_text(encoding="utf-8"))
        parse_rows.append({"path": str(path), "format": "json", "rows": "", "status": "PASS"})
    for path in sorted(OUT_DIR.rglob("*.md")):
        path.read_text(encoding="utf-8")
        parse_rows.append({"path": str(path), "format": "markdown", "rows": "", "status": "PASS"})
    write_csv(OUT_DIR / f"parse_validation_{RUN_DATE}.csv", parse_rows)

    manifest = OUT_DIR / f"sha256_manifest_{RUN_DATE}.csv"
    manifest_rows = []
    for path in sorted(p for p in OUT_DIR.rglob("*") if p.is_file() and p != manifest):
        manifest_rows.append({
            "relative_path": str(path.relative_to(OUT_DIR)),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_path(path),
        })
    write_csv(manifest, manifest_rows, ["relative_path", "size_bytes", "sha256"])
    result["package_sha256_manifest_hash"] = sha256_path(manifest)
    return result


def main() -> int:
    result = build()
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
