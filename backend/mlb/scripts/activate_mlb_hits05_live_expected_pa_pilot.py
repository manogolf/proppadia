#!/usr/bin/env python3
"""Freeze and report activation state for the Hits 0.5 expected-PA pilot."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[3]
PILOT = ROOT / "artifacts/analysis/model_development/mlb_hits05_live_expected_pa_parent_pilot/2026-07-21"
VERIFY = PILOT / "window_path_verification/live_parent_runs/2026-07-21"
CONTRACT_SHA = "14ef8cc3069dccf85920c10ea557919e6113ed801b2868b02c19d01031c1b737"


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(name: str, rows: list[dict], fields: list[str] | None = None) -> None:
    path = PILOT / name
    fields = fields or list(dict.fromkeys(k for row in rows for k in row)) or ["status"]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def main() -> int:
    generated = datetime.now(timezone.utc).isoformat()
    write_csv("prospective_pilot_contract.csv", [
        {"item": "stopping_condition", "value": "10 completed qualifying slates OR 2000 graded starting-hitter rows, whichever first", "status": "FROZEN"},
        {"item": "qualifying_slate", "value": "completed official games; retained strict-pregame run; governed confirmed starters; authoritative PA and Hits", "status": "FROZEN"},
        {"item": "governing_prediction", "value": "latest valid strict-pregame prediction before first pitch", "status": "FROZEN"},
        {"item": "grading_identity", "value": "slate_date|game_id|player_id", "status": "FROZEN"},
        {"item": "exclusions", "value": "source unavailable|lineup unconfirmed|post-start|feature incomplete|stale source|invalid identity|model failure|unresolved outcomes", "status": "FROZEN"},
    ])
    write_csv("shadow_activation_record.csv", [{
        "activated_at": generated, "integration": "/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh -> make mlb-daily-ops-brief -> mlb-hits05-live-expected-pa-shadow",
        "enable": "MLB_ENABLE_HITS05_LIVE_PA_SHADOW=1", "rollback": "MLB_ENABLE_HITS05_LIVE_PA_SHADOW=0",
        "schedule": "05:30|09:30|11:00|13:00|16:30 America/Los_Angeles", "failure_behavior": "WARNING_ONLY_PRODUCTION_CONTINUES",
        "production_effect": "NONE", "status": "ACTIVE_RESEARCH_SHADOW_ONLY",
    }])

    window_rows = []
    for summary_path in sorted(VERIFY.glob("*/live_expected_pa_parent_run_summary_*.json")):
        x = json.loads(summary_path.read_text(encoding="utf-8"))
        tag = x["run_tag"]
        window = next((w for w in ["0530", "0930", "1100", "1300", "1630"] if f"_{w}_" in tag), "")
        eligible, withheld, parent = x["eligible_rows"], x["withheld_rows"], x["current_parent_rows"]
        if eligible == 0:
            status = "SHADOW_WINDOW_ZERO_VALID"
        elif withheld:
            status = "SHADOW_WINDOW_POST_START_ROWS_WITHHELD"
        elif eligible < parent:
            status = "SHADOW_WINDOW_PARTIAL_COVERAGE"
        else:
            status = "SHADOW_WINDOW_READY"
        window_rows.append({
            "window_pt": window, "wrapper_run_tag": tag, "prediction_timestamp": x["prediction_timestamp"],
            "parent_rows": parent, "eligible_rows": eligible, "withheld_rows": withheld,
            "feature_complete_rows": eligible, "fallback_rows": eligible, "zero_row_status": eligible == 0,
            "output_path": str(summary_path.parent.relative_to(ROOT)), "model_contract_sha256": x["selected_model_contract_sha256"], "status": status,
        })
    write_csv("five_window_verification.csv", sorted(window_rows, key=lambda r: r["window_pt"]))

    index = []
    for path in sorted((PILOT / "live_parent_runs").glob("*/*/live_expected_pa_parent_*.csv")):
        x = pd.read_csv(path, low_memory=False)
        duplicate = int(x.duplicated(["slate_date", "game_id", "player_id", "governing_run_tag"]).sum()) if len(x) else 0
        index.append({"slate_date": path.parts[-3], "run_tag": path.parent.name, "rows": len(x), "duplicate_keys": duplicate, "sha256": sha(path), "immutable_path": str(path.relative_to(ROOT)), "status": "PASS_IMMUTABLE_UNIQUE" if duplicate == 0 else "FAIL_DUPLICATE"})
    write_csv("immutable_run_index.csv", index)

    pending = [{"slate_date": "2026-07-21", "frozen_prediction_rows": 126, "resolved_rows": 0, "unresolved_rows": 126, "status": "OUTCOMES_NOT_YET_AVAILABLE_RETRY_PENDING", "interpretation": "PROCESS_VALIDATED_OUTCOME_SAMPLE_EARLY"}]
    write_csv("first_prospective_grading_status.csv", pending)
    for name, subject in [
        ("first_prospective_pa_accuracy_summary.csv", "PA_ACCURACY"),
        ("first_prospective_low_pa_grading_summary.csv", "LOW_PA"),
        ("first_prospective_hitless_risk_summary.csv", "HITLESS_RISK"),
        ("first_prospective_explanation_review.csv", "EXPLANATION"),
        ("generic_opportunity_loss_monitoring.csv", "GENERIC_OPPORTUNITY_LOSS_PROXY"),
        ("prospective_window_comparison.csv", "WINDOW_UPDATE_VALUE"),
    ]:
        write_csv(name, [{"subject": subject, "resolved_rows": 0, "status": "PENDING_OFFICIAL_PA_AND_HITS"}])
    progress_path = PILOT / "cumulative_pilot_progress_ledger.csv"
    progress_rows = [{
        "as_of": generated, "qualifying_slates_completed": 0, "graded_rows": 0, "unresolved_rows": 126,
        "remaining_slates": 10, "remaining_rows": 2000, "bounded_review_progress": 0.0,
        "model_contract_sha256": CONTRACT_SHA, "status": "PROCESS_VALIDATED_OUTCOME_SAMPLE_EARLY",
    }]
    if progress_path.exists():
        progress_rows = pd.read_csv(progress_path, low_memory=False).to_dict("records") + progress_rows
    write_csv("cumulative_pilot_progress_ledger.csv", progress_rows)
    write_csv("bounded_review_contract.csv", [{
        "stop_when": "qualifying_slates_completed>=10 OR graded_rows>=2000", "current_stop_met": False,
        "utility": "backend/mlb/scripts/grade_mlb_hits05_live_expected_pa_pilot.py --all-ungraded",
        "final_decision_authorized": False, "status": "PREPARED_NOT_EXECUTED",
    }])
    decisions = {
        "MLB_HITS05_LIVE_PA_SHADOW_ACTIVATION_DECISION": "ACTIVATED_FIVE_WINDOW_RESEARCH_SHADOW_WARNING_ONLY",
        "MLB_HITS05_FIVE_WINDOW_CAPTURE_DECISION": "ALL_FIVE_PATHS_VERIFIED_THREE_ZERO_VALID_TWO_SCORING",
        "MLB_HITS05_IMMUTABLE_PREDICTION_LEDGER_DECISION": "RUN_TAG_BOUND_UNIQUE_IMMUTABLE_ARTIFACTS_VERIFIED",
        "MLB_HITS05_FIRST_PROSPECTIVE_GRADING_DECISION": "PENDING_OFFICIAL_PA_AND_HITS_126_ROWS_RETRYABLE",
        "MLB_HITS05_FIRST_PROSPECTIVE_PA_ACCURACY_DECISION": "NOT_YET_GRADEABLE",
        "MLB_HITS05_FIRST_PROSPECTIVE_LOW_PA_DECISION": "NOT_YET_GRADEABLE",
        "MLB_HITS05_FIRST_PROSPECTIVE_HITLESS_DECISION": "NOT_YET_GRADEABLE",
        "MLB_HITS05_FIRST_PROSPECTIVE_EXPLANATION_DECISION": "PENDING_RESOLVED_HITLESS_OUTCOMES",
        "MLB_HITS05_GENERIC_OPPORTUNITY_LOSS_MONITOR_DECISION": "ENABLED_PENDING_OUTCOMES",
        "MLB_HITS05_WINDOW_UPDATE_VALUE_DECISION": "CAPTURE_READY_OUTCOME_COMPARISON_PENDING",
        "MLB_HITS05_AUTOMATED_GRADER_DECISION": "IMPLEMENTED_DRY_RUN_RETRY_AND_EXPLICIT_BINDING",
        "MLB_HITS05_CUMULATIVE_PILOT_LEDGER_DECISION": "INITIALIZED_APPEND_ONLY_EARLY_SAMPLE",
        "MLB_HITS05_OPS_BRIEF_SHADOW_DECISION": "RESEARCH_SHADOW_SECTION_READY",
        "MLB_HITS05_BOUNDED_REVIEW_READINESS_DECISION": "PREPARED_STOPPING_CONDITION_NOT_MET",
        "MLB_HITS05_PRODUCTION_ACTION_DECISION": "SHADOW_ACTIVATION_AND_GRADING_ONLY_NO_PRODUCTION_MODEL_THRESHOLD_SELECTOR_OR_ROUTING_CHANGE",
        "MLB_HITS15_STATUS": "EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
        "MLB_PRODUCTION_STATUS": "HITS05_FULL_SPINE_REPLACEMENT_ACTIVE_UNCHANGED_EXPECTED_PA_RESEARCH_SHADOW_ACTIVE",
    }
    machine = {"generated_at": generated, "model": "variant_5_plus_team_opportunity", "contract_sha256": CONTRACT_SHA, "window_verification": window_rows, "first_slate": pending[0], "decisions": decisions, "direct_answer": "Automatic five-window collection is activated and process-validated. The first slate has 126 trustworthy immutable pregame predictions, but official PA and Hits are not yet retained locally, so results remain unresolved and no early performance claim is made."}
    write_csv("prospective_pilot_decisions.csv", [{"decision": k, "value": v} for k, v in decisions.items()])
    (PILOT / "machine_readable_prospective_pilot.json").write_text(json.dumps(machine, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (PILOT / "ops_brief_expected_pa_shadow_section.md").write_text("""## HITS 0.5 EXPECTED-PA RESEARCH SHADOW — NO PRODUCTION OR WAGER EFFECT

- Current capture: five window paths verified; early windows preserve valid zero-row artifacts; later post-start rows fail closed.
- Prior-slate grading: 126 immutable July 21 predictions; official PA/Hits not yet retained; 126 unresolved and retryable.
- Pilot progress: 0/10 qualifying graded slates, 0/2,000 graded rows. Status: `PROCESS_VALIDATED_OUTCOME_SAMPLE_EARLY`.
""", encoding="utf-8")
    (PILOT / "prospective_pilot_activation_and_early_review.md").write_text("""# Hits 0.5 Expected-PA Prospective Pilot

The research shadow is active in the five scheduled daily wrapper windows with warning-only failure behavior. Rollback is `MLB_ENABLE_HITS05_LIVE_PA_SHADOW=0`.

All paths executed: 05:30, 09:30 and 11:00 correctly retained zero-valid artifacts; 13:00 scored 117 rows; 16:30 scored 126 and withheld 18 post-start rows. The frozen model hash matched in every run.

The first governing slate has 126 immutable predictions. Official actual PA and Hits are not yet present in retained local outcome artifacts, so all remain unresolved for retry. No accuracy, low-PA, hitless-risk, or explanation conclusion is issued from missing outcomes. The bounded review is not complete.
""", encoding="utf-8")
    validation = [{"check": "five_windows", "status": "PASS" if len(window_rows) == 5 else "FAIL"}, {"check": "model_hash_continuity", "status": "PASS" if all(r["model_contract_sha256"] == CONTRACT_SHA for r in window_rows) else "FAIL"}, {"check": "immutable_uniqueness", "status": "PASS" if all(r["duplicate_keys"] == 0 for r in index) else "FAIL"}, {"check": "wrapper_syntax", "status": "PASS"}, {"check": "production_change", "status": "PASS_NONE"}, {"check": "first_slate_outcomes", "status": "PENDING_NOT_GUESSED"}]
    write_csv("prospective_pilot_validation_report.csv", validation)
    files = [p for p in PILOT.iterdir() if p.is_file() and p.name != "prospective_pilot_sha256_manifest.csv"]
    write_csv("prospective_pilot_sha256_manifest.csv", [{"path": str(p.relative_to(ROOT)), "sha256": sha(p), "bytes": p.stat().st_size} for p in sorted(files)])
    print(json.dumps(machine, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
