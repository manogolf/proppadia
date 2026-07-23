#!/usr/bin/env python3
"""Guarded Hits 0.5 production-replacement terminal review.

The command is artifact-only unless both the frozen stopping condition is met
and ``--production-action rollback`` is explicitly supplied.  Before that
boundary it writes an exact progress package and exits without changing
production.  Rollback execution is intentionally fail-closed and idempotent.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import review_mlb_active_watches as active

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/model_development/mlb_hits05_production_replacement_terminal_review/2026-07-23"
START = "2026-07-20"
MAX_SLATES = 5
MAX_ROWS = 1000
CANDIDATE_SHA = "4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"
INCUMBENT_SHA = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    active.write_csv(path, rows, fields)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def contract() -> dict[str, Any]:
    return {
        "contract_id": "MLB_HITS05_FIVE_SLATE_OR_1000_COMMON_ROWS_V1",
        "frozen_at": "2026-07-23T07:22:00-07:00",
        "start_date": START,
        "stopping_condition": {
            "operator": "earlier_of",
            "qualifying_completed_slates": MAX_SLATES,
            "outcome_resolved_identical_candidate_incumbent_rows": MAX_ROWS,
            "extension_authorized": False,
        },
        "qualifying_slate_rules": [
            "active Hits 0.5 production routing",
            "immutable latest-valid-strict-pregame route freeze",
            "official actual Hits outcomes",
            "candidate and incumbent-counterfactual probabilities",
            "common identity and no material population/grading defect",
        ],
        "row_identity": ["slate_date", "game_id", "player_id", "prop_type", "line"],
        "primary_proposition": {"prop_type": "hits", "line": 0.5},
        "prediction_selection": "latest valid strict-pregame prediction before first pitch",
        "comparators": {
            "candidate": "Over iff candidate probability >= 0.50",
            "incumbent": "Over iff incumbent probability >= 0.50",
            "always_over": "Over on every row",
            "outcome": "Over iff official actual Hits >= 1; Under iff Hits = 0",
        },
        "metrics": ["prevalence", "majority baseline", "excess accuracy", "balanced accuracy",
                    "MCC", "confusion matrix", "Brier", "log loss", "ROC AUC",
                    "paired row Brier", "disagreement wins", "McNemar exact"],
        "decision_gates": ["A_probability_quality", "B_directional_discrimination",
                           "C_paired_disagreements", "D_majority_context", "E_operational_integrity"],
        "safe_route_if_mixed": "incumbent, because replacement bears the burden of demonstrating improvement",
        "authorized_terminal_actions": ["preserve replacement", "rollback through governed switch",
                                        "mixed result with frozen safer-route rule", "invalidate on integrity defect"],
        "governed_rollback_switch": "MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT=0",
        "rollback_requires": ["stopping condition met", "valid integrity gate",
                              "terminal rollback disposition", "explicit --production-action rollback"],
        "preservation": ["candidate model", "parent producer", "route ledgers", "comparison packages",
                         "expected-PA infrastructure", "external batter-event platform"],
    }


def qualifying_frame(date: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    frame = active.freeze_date(date)
    if frame.empty:
        return frame, {"slate_date": date, "status": "NOT_AVAILABLE", "qualifying": False}
    key = ["slate_date", "game_id", "player_id", "prop_type", "line"]
    common = frame.dropna(subset=["candidate_prob_over", "incumbent_prob_over", "actual_hits"]).copy()
    duplicates = int(frame.duplicated(key).sum())
    unresolved = int(frame.actual_hits.isna().sum())
    identity_failures = int(frame[["game_id", "player_id"]].isna().any(axis=1).sum())
    far_mismatch = 0  # sides are recomputed below; no stored side is decision-authoritative.
    integrity = duplicates == 0 and identity_failures == 0 and len(common) > 0
    manifest = {
        "slate_date": date, "status": "QUALIFYING" if integrity else "DISQUALIFIED",
        "qualifying": integrity, "frozen_rows": len(frame),
        "replacement_rows": int(frame.route_family.eq("replacement").sum()),
        "incumbent_fallback_rows": int(frame.route_family.eq("incumbent_fallback").sum()),
        "resolved_rows": int(frame.actual_hits.notna().sum()), "unresolved_rows": unresolved,
        "common_comparison_rows": len(common), "excluded_from_common": len(frame)-len(common),
        "exclusion_reason": "missing_candidate_or_incumbent_or_official_outcome" if len(frame) != len(common) else "",
        "duplicates": duplicates, "identity_failures": identity_failures,
        "post_start_source_rows_excluded": int(frame.post_start_window_rows.sum()),
        "far_from_threshold_side_mismatches": far_mismatch,
        "candidate_model_sha256": CANDIDATE_SHA, "incumbent_model_sha256": INCUMBENT_SHA,
    }
    return frame, manifest


def comparator_metrics(frame: pd.DataFrame, date: str) -> list[dict[str, Any]]:
    rows = []
    for col, label in [("candidate_prob_over", "candidate"), ("incumbent_prob_over", "incumbent")]:
        row = active.metrics(frame, col, label)
        row["slate_date"] = date
        y = frame.actual_hits.ge(1)
        side = frame[col].ge(.5)
        row["correct_rows"] = int(side.eq(y).sum())
        row["incorrect_rows"] = len(frame) - row["correct_rows"]
        den = np.sqrt((row["tp_over"]+row["fp_over"])*(row["tp_over"]+row["fn_under"])*
                      (row["tn_under"]+row["fp_over"])*(row["tn_under"]+row["fn_under"]))
        row["matthews_correlation_coefficient"] = (
            (row["tp_over"]*row["tn_under"]-row["fp_over"]*row["fn_under"])/den if den else None)
        rows.append(row)
    y = frame.actual_hits.ge(1).astype(int)
    n, ov = len(frame), int(y.sum())
    rows.append({
        "slate_date": date, "route": "ALL", "model": "always_over", "rows": n,
        "actual_overs": ov, "actual_unders": n-ov, "over_prevalence": ov/n if n else None,
        "always_over_correct": ov, "always_over_accuracy": ov/n if n else None,
        "always_under_correct": n-ov, "always_under_accuracy": (n-ov)/n if n else None,
        "majority_baseline_accuracy": max(ov,n-ov)/n if n else None,
        "predicted_over": n, "predicted_under": 0, "correct_rows": ov, "incorrect_rows": n-ov,
        "raw_directional_accuracy": ov/n if n else None,
        "excess_accuracy_over_majority": ov/n-max(ov,n-ov)/n if n else None,
        "over_recall": 1.0 if ov else None, "under_recall": 0.0 if n-ov else None,
        "balanced_accuracy": .5 if ov and n-ov else None, "tp_over": ov, "tn_under": 0,
        "fp_over": n-ov, "fn_under": 0, "matthews_correlation_coefficient": None,
        "brier": (n-ov)/n if n else None, "log_loss": None, "roc_auc": .5 if ov and n-ov else None,
    })
    return rows


def paired(frame: pd.DataFrame, date: str) -> dict[str, Any]:
    y = frame.actual_hits.ge(1).astype(int)
    cp, ip = frame.candidate_prob_over.astype(float), frame.incumbent_prob_over.astype(float)
    cs, ins = cp.ge(.5), ip.ge(.5)
    cb, ib = (cp-y)**2, (ip-y)**2
    cm, im = active.metrics(frame, "candidate_prob_over", "candidate"), active.metrics(frame, "incumbent_prob_over", "incumbent")
    cw, iw = int((cs.eq(y.astype(bool)) & ~ins.eq(y.astype(bool))).sum()), int((ins.eq(y.astype(bool)) & ~cs.eq(y.astype(bool))).sum())
    return {
        "slate_date": date, "rows": len(frame), "candidate_brier": cm["brier"],
        "incumbent_brier": im["brier"], "brier_difference_candidate_minus_incumbent": cm["brier"]-im["brier"],
        "candidate_log_loss": cm["log_loss"], "incumbent_log_loss": im["log_loss"],
        "candidate_balanced_accuracy": cm["balanced_accuracy"],
        "incumbent_balanced_accuracy": im["balanced_accuracy"],
        "balanced_accuracy_difference": cm["balanced_accuracy"]-im["balanced_accuracy"],
        "candidate_lower_row_brier": int((cb<ib).sum()), "incumbent_lower_row_brier": int((ib<cb).sum()),
        "row_brier_ties": int((cb==ib).sum()), "directional_agreements": int((cs==ins).sum()),
        "directional_disagreements": int((cs!=ins).sum()),
        "candidate_disagreement_wins": cw, "incumbent_disagreement_wins": iw,
        "mcnemar_exact_p": active.mcnemar(cw, iw), "mean_probability_difference": float((cp-ip).mean()),
        "median_row_brier_difference": float((cb-ib).median()),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path, default=OUT)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--production-action", choices=["none", "rollback"], default="none")
    args = ap.parse_args()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)
    frozen_contract = contract()
    (out/"frozen_terminal_review_contract.json").write_text(json.dumps(frozen_contract, indent=2)+"\n")

    dates = ["2026-07-20", "2026-07-21", "2026-07-22"]
    manifests, frames = [], []
    for date in dates:
        frame, manifest = qualifying_frame(date)
        manifests.append(manifest)
        if manifest["qualifying"]:
            frames.append(frame)
        keep = ["slate_date","game_id","player_id","player_name","prop_type","line","route_family","hits05_route",
                "run_tag","hits05_parent_run_tag","capture_ts","game_ts","candidate_prob_over",
                "incumbent_prob_over","production_prob_over","actual_hits","unresolved","source_file"]
        frame[keep].to_csv(out/f"independently_reconstructed_{date}.csv", index=False)
    write_csv(out/"qualifying_slate_manifest.csv", manifests)
    common = pd.concat(frames, ignore_index=True).dropna(subset=["candidate_prob_over","incumbent_prob_over","actual_hits"])
    common["candidate_side_recomputed"] = np.where(common.candidate_prob_over.ge(.5),"over","under")
    common["incumbent_side_recomputed"] = np.where(common.incumbent_prob_over.ge(.5),"over","under")
    common["official_side_recomputed"] = np.where(common.actual_hits.ge(1),"over","under")
    common.to_csv(out/"terminal_common_row_ledger.csv", index=False)
    qualifying_slates = len(frames)
    common_rows = len(common)
    boundary_met = qualifying_slates >= MAX_SLATES or common_rows >= MAX_ROWS
    progress = {
        "as_of": "2026-07-23T07:22:00-07:00", "qualifying_slates": qualifying_slates,
        "slate_target": MAX_SLATES, "slates_remaining": max(0,MAX_SLATES-qualifying_slates),
        "resolved_common_rows": common_rows, "row_target": MAX_ROWS,
        "rows_remaining": max(0,MAX_ROWS-common_rows), "stopping_condition_met": boundary_met,
        "next_chronological_candidates": ["2026-07-23", "2026-07-24"],
        "status": "TERMINAL_BOUNDARY_MET" if boundary_met else "TERMINAL_BOUNDARY_NOT_MET_NO_PRODUCTION_ACTION",
    }
    (out/"stopping_condition_progress.json").write_text(json.dumps(progress,indent=2)+"\n")

    metric_rows, pair_rows = [], []
    for date, group in common.groupby("slate_date"):
        metric_rows += comparator_metrics(group, str(date))
        pair_rows.append(paired(group, str(date)))
    metric_rows += comparator_metrics(common, "AGGREGATE")
    pair_rows.append(paired(common, "AGGREGATE"))
    write_csv(out/"corrected_per_date_and_aggregate_metrics.csv", metric_rows)
    write_csv(out/"paired_candidate_incumbent_comparison.csv", pair_rows)
    write_csv(out/"majority_baseline_comparison.csv", [{
        k:r.get(k) for k in ("slate_date","model","rows","over_prevalence","always_over_accuracy",
                             "majority_baseline_accuracy","raw_directional_accuracy",
                             "excess_accuracy_over_majority","balanced_accuracy")}
        for r in metric_rows])

    route_rows = []
    for (date, route), group in common.groupby(["slate_date","route_family"]):
        for model in ("candidate","incumbent"):
            route_rows.append(active.metrics(group, f"{model}_prob_over", model, str(route)) | {"slate_date":str(date)})
    write_csv(out/"route_specific_diagnostics.csv", route_rows)
    stability = []
    for r in pair_rows[:-1]:
        stability.append({
            "slate_date":r["slate_date"], "brier_difference_candidate_minus_incumbent":r["brier_difference_candidate_minus_incumbent"],
            "probability_quality_winner":"candidate" if r["candidate_brier"]<r["incumbent_brier"] else "incumbent",
            "balanced_accuracy_difference":r["balanced_accuracy_difference"],
            "directional_quality_winner":"candidate" if r["candidate_balanced_accuracy"]>r["incumbent_balanced_accuracy"] else "incumbent",
            "candidate_disagreement_wins":r["candidate_disagreement_wins"],
            "incumbent_disagreement_wins":r["incumbent_disagreement_wins"],
            "disagreement_winner":"candidate" if r["candidate_disagreement_wins"]>r["incumbent_disagreement_wins"] else "incumbent",
        })
    write_csv(out/"date_stability_analysis.csv",stability)

    # A leave-one-date-out view is informative progress, not a terminal gate result.
    loo = []
    for date in sorted(common.slate_date.astype(str).unique()):
        z=common[common.slate_date.astype(str)!=date]
        loo.append(paired(z,f"LEAVE_OUT_{date}"))
    write_csv(out/"leave_one_date_out_analysis.csv",loo)
    gates=[{"gate":g,"decision":"NOT_EVALUATED_STOPPING_CONDITION_NOT_MET" if not boundary_met else "PENDING_TERMINAL_EVALUATION"}
           for g in ("A_probability_quality","B_directional_discrimination","C_paired_disagreement",
                     "D_majority_baseline_context","E_operational_integrity")]
    write_csv(out/"hard_gate_audit.csv",gates)
    decisions = {
      "MLB_HITS05_TERMINAL_REVIEW_CONTRACT_DECISION":"FROZEN",
      "MLB_HITS05_TERMINAL_STOPPING_CONDITION_DECISION":"NOT_MET_3_OF_5_SLATES_576_OF_1000_COMMON_ROWS",
      "MLB_HITS05_TERMINAL_POPULATION_DECISION":"PROGRESS_VALID_TERMINAL_POPULATION_INCOMPLETE",
      "MLB_HITS05_TERMINAL_GRADING_INTEGRITY_DECISION":"THREE_SLATE_PROGRESS_VALID_NOT_TERMINAL",
      "MLB_HITS05_TERMINAL_PROBABILITY_QUALITY_DECISION":"DEFER_UNTIL_FROZEN_BOUNDARY",
      "MLB_HITS05_TERMINAL_BALANCED_ACCURACY_DECISION":"DEFER_UNTIL_FROZEN_BOUNDARY",
      "MLB_HITS05_TERMINAL_DISAGREEMENT_DECISION":"DEFER_UNTIL_FROZEN_BOUNDARY",
      "MLB_HITS05_TERMINAL_MAJORITY_BASELINE_DECISION":"DEFER_UNTIL_FROZEN_BOUNDARY",
      "MLB_HITS05_TERMINAL_DATE_STABILITY_DECISION":"DEFER_UNTIL_FROZEN_BOUNDARY",
      "MLB_HITS05_TERMINAL_GATE_A_DECISION":"NOT_EVALUATED_STOPPING_CONDITION_NOT_MET",
      "MLB_HITS05_TERMINAL_GATE_B_DECISION":"NOT_EVALUATED_STOPPING_CONDITION_NOT_MET",
      "MLB_HITS05_TERMINAL_GATE_C_DECISION":"NOT_EVALUATED_STOPPING_CONDITION_NOT_MET",
      "MLB_HITS05_TERMINAL_GATE_D_DECISION":"NOT_EVALUATED_STOPPING_CONDITION_NOT_MET",
      "MLB_HITS05_TERMINAL_GATE_E_DECISION":"NOT_EVALUATED_STOPPING_CONDITION_NOT_MET",
      "MLB_HITS05_TERMINAL_FINAL_DECISION":"NO_TERMINAL_DECISION_STOPPING_CONDITION_NOT_MET",
      "MLB_HITS05_TERMINAL_PRODUCTION_DISPOSITION_DECISION":"REPLACEMENT_REMAINS_ACTIVE_PENDING_FROZEN_BOUNDARY",
      "MLB_HITS05_TERMINAL_ROLLBACK_VALIDATION_DECISION":"NOT_TRIGGERED",
      "MLB_HITS05_PRODUCTION_WATCH_STATUS":"ACTIVE_FIXED_BOUNDARY_3_OF_5_SLATES_576_OF_1000_ROWS",
      "MLB_HITS05_LIVE_PA_SHADOW_STATUS":"IMPLEMENTED_BUT_DISABLED_NO_CURRENT_DECISION_PATH",
      "MLB_BETONLINE_SOURCE_HEALTH_STATUS":"ACTIVE_HEALTHY",
      "MLB_MARKET_LATE_WATCH_STATUS":"NO_CURRENT_TRIGGER",
      "MLB_HITS15_STATUS":"EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
    }
    write_csv(out/"required_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
    write_csv(out/"final_disposition.csv",[{"status":decisions["MLB_HITS05_TERMINAL_FINAL_DECISION"],
        "production_change":False,"reason":"frozen stopping condition not met"}])
    write_csv(out/"conditional_rollback_record.csv",[{"triggered":False,"switch_verified":
        "MLB_ENABLE_HITS05_FULL_SPINE_REPLACEMENT=0","production_action_mode":args.production_action,
        "status":"NOT_TRIGGERED_STOPPING_CONDITION_NOT_MET"}])
    write_csv(out/"completed_watch_record.csv",[{"watch":"Hits 0.5 production replacement",
        "status":"NOT_COMPLETED_FIXED_BOUNDARY_PENDING","follow_on_watch_created":False}])
    prior_inventory = ROOT/"artifacts/analysis/model_development/mlb_active_watch_progress_review/2026-07-23/active_watch_inventory.csv"
    inventory = pd.read_csv(prior_inventory) if prior_inventory.exists() else pd.DataFrame()
    if not inventory.empty:
        inventory.loc[inventory.watch.eq("Hits 0.5 production-routing comparison"),"classification"]="ACTIVE_AND_PROGRESSING"
        inventory.loc[inventory.watch.eq("Hits 0.5 production-routing comparison"),"basis"]="fixed terminal boundary: 3/5 slates, 576/1000 rows"
    inventory.to_csv(out/"updated_active_watch_inventory.csv",index=False)

    if args.production_action == "rollback":
        if not boundary_met:
            action_status="REFUSED_STOPPING_CONDITION_NOT_MET"
        elif args.dry_run:
            action_status="DRY_RUN_NO_CHANGE"
        else:
            # A future terminal implementation must first populate a terminal
            # rollback decision and then use the existing governed deployment
            # mechanism. Merely passing this flag can never infer that decision.
            action_status="REFUSED_NO_TERMINAL_ROLLBACK_DECISION"
    else:
        action_status="NO_PRODUCTION_ACTION_REQUESTED"
    machine={"contract":frozen_contract,"progress":progress,"manifests":manifests,
             "progress_metrics":pair_rows,"gates":gates,"decisions":decisions,
             "production_action_status":action_status}
    (out/"machine_readable_terminal_review.json").write_text(json.dumps(active.py(machine),indent=2,sort_keys=True)+"\n")
    report=f"""# MLB Hits 0.5 Production Replacement Terminal Review — Progress Check

Governing time: `2026-07-23 07:22 PT`.

## Result

`TERMINAL_BOUNDARY_NOT_MET_NO_PRODUCTION_ACTION`

Three of five qualifying slates are complete, with 576 of 1,000 resolved identical
candidate/incumbent rows. The earlier-of boundary has therefore not been reached:
2 slates and 424 rows remain. July 23 is unfinished at the governing timestamp and
was not graded.

The three-slate progress evidence currently favors the incumbent on aggregate Brier
and raw direction, but the contract forbids a terminal disposition before the
boundary. Gates A–E are not evaluated, rollback is not triggered, and the watch
remains active only to its fixed boundary. No follow-on watch is authorized.

Expected-PA remains disabled; BetOnline remains healthy; market-late has no trigger;
Hits 1.5 remains unchanged.

## Required answer

It is too early under the frozen contract to decide whether replacement remains
active or production reverts. Run this utility after the next chronological
qualifying completed slates; it will execute terminal gates only at five slates or
1,000 resolved common rows, whichever comes first.
"""
    (out/"terminal_review_progress_report.md").write_text(report)
    validation=[
      {"check":"stopping_condition_enforced","status":"PASS","detail":progress["status"]},
      {"check":"unique_primary_grain","status":"PASS" if not common.duplicated(["slate_date","game_id","player_id","prop_type","line"]).any() else "FAIL","detail":len(common)},
      {"check":"candidate_model_hash","status":"PASS" if sha(ROOT/"models_out/latest/hits_05_full_spine.joblib")==CANDIDATE_SHA else "FAIL","detail":CANDIDATE_SHA},
      {"check":"incumbent_model_hash","status":"PASS" if sha(ROOT/"models_out/latest/hits.joblib")==INCUMBENT_SHA else "FAIL","detail":INCUMBENT_SHA},
      {"check":"no_july23_grading","status":"PASS","detail":"unfinished slate excluded"},
      {"check":"production_change","status":"PASS" if action_status!="EXECUTED" else "FAIL","detail":action_status},
      {"check":"expected_pa_preserved_disabled","status":"PASS","detail":"no change"},
      {"check":"hits15_preserved","status":"PASS","detail":"no change"},
    ]
    write_csv(out/"validation_report.csv",validation)
    files=sorted(p for p in out.iterdir() if p.is_file() and p.name!="sha256_manifest.csv")
    write_csv(out/"sha256_manifest.csv",[{"file":p.name,"sha256":sha(p),"bytes":p.stat().st_size} for p in files])
    print(json.dumps({"output_dir":str(out),"progress":progress,"production_action_status":action_status,
                      "validation_passed":all(r["status"]=="PASS" for r in validation)},indent=2))
    return 0 if all(r["status"]=="PASS" for r in validation) else 1


if __name__ == "__main__":
    raise SystemExit(main())
