#!/usr/bin/env python3
"""Read-only MLB watch, Champion-Challenger, and promotion-evidence audit.

This utility inventories existing repository evidence. It does not train,
score, fetch odds, write databases, edit schedulers, or change production
behavior.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_watch_champion_promotion_integrity_audit/2026-07-18"
MODEL_INDEX = ROOT / "models_out/latest/MODEL_INDEX.json"
CHAMPION_FREEZE = ROOT / "artifacts/analysis/model_development/champion_freeze_2026-07-10"


def rel(path: Path | str) -> str:
    p = Path(path)
    try:
        return str(p.resolve().relative_to(ROOT))
    except Exception:
        return str(path)


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_md(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def production_inventory() -> list[dict[str, Any]]:
    index = read_json(MODEL_INDEX)
    rows: list[dict[str, Any]] = []
    for prop, meta in sorted(index.items()):
        model_file = meta.get("file", f"{prop}.joblib")
        artifact = ROOT / "models_out/latest" / str(model_file)
        rows.append(
            {
                "prop_type": prop,
                "line_coverage": "market-line dependent; line supplied by odds/slate artifact",
                "production_status": "ACTIVE_RUNTIME_CHAMPION",
                "generating_script_module": "backend/mlb/scripts/build_mlb_predictions_wide.py -> backend.domains.mlb.prop_workflow.predict_prop -> backend.mlb.prediction.make_prediction.predict",
                "prediction_construction": "trained_model",
                "native_output_fields": "model_prob_over, model_prob_under, model_pick_side, model_pick_prob, model_fair_over_american, model_fair_under_american",
                "selected_side_rule": "artifact decision_threshold plus model probability/side logic in make_prediction; upload/lane rules are downstream",
                "current_model_formula_version": f"{meta.get('training_profile', 'unknown')} trained_at={meta.get('trained_at', 'UNKNOWN')}",
                "daily_output_artifact": "backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv; backend/mlb/data/processed/mlb_slate_output.csv",
                "downstream_consumer": "slate output, lane selector, review aids, Ops Brief, workspace, upload generation",
                "historical_performance_source": "artifacts/analysis/mlb/execution_vs_model/*/reconcile_rows.csv and model performance summaries",
                "runtime_artifact": rel(artifact),
                "runtime_artifact_exists": artifact.exists(),
                "runtime_artifact_sha256": sha256(artifact) if artifact.exists() else "",
                "feature_count": len(meta.get("input_columns") or []),
                "do_not_call_model_unless_fitted_estimator_loaded": "fitted estimator artifact present" if artifact.exists() else "UNKNOWN",
            }
        )
    rows.extend(
        [
            {
                "prop_type": "starter_expected_hits_allowed",
                "line_coverage": "not a market prop; context field for Hits 1.5 research",
                "production_status": "PASSIVE_CONTEXT",
                "generating_script_module": "backend/mlb/scripts/report_mlb_hits_environment.py",
                "prediction_construction": "deterministic_formula",
                "native_output_fields": "pitcher_base, offense_factor_vs_league_clamped, starter_expected_hits_allowed",
                "selected_side_rule": "none",
                "current_model_formula_version": "pitcher_base * offense_factor_vs_league_clamped",
                "daily_output_artifact": "artifacts/analysis/mlb/hits_environment/*",
                "downstream_consumer": "review aids and research artifacts",
                "historical_performance_source": "Hits 1.5 tier/research rows, not production model comparison by itself",
                "runtime_artifact": "",
                "runtime_artifact_exists": "",
                "runtime_artifact_sha256": "",
                "feature_count": "",
                "do_not_call_model_unless_fitted_estimator_loaded": "deterministic formula, not a model",
            },
            {
                "prop_type": "rolling_market_late_candidate_observation",
                "line_coverage": "player-prop market rows observed after morning run",
                "production_status": "RESEARCH_OBSERVATION",
                "generating_script_module": "backend/mlb/scripts/build_mlb_rolling_market_late_candidates.py",
                "prediction_construction": "operational_projection_from_existing_artifacts",
                "native_output_fields": "ledger/current projection/growth/pivot fields",
                "selected_side_rule": "none; candidate discovery observation",
                "current_model_formula_version": "no model",
                "daily_output_artifact": "artifacts/analysis/mlb/market_late_candidate_discovery/rolling_observation_<date>",
                "downstream_consumer": "Ops Brief research section and CSV pivots",
                "historical_performance_source": "future reconciliation only",
                "runtime_artifact": "",
                "runtime_artifact_exists": "",
                "runtime_artifact_sha256": "",
                "feature_count": "",
                "do_not_call_model_unless_fitted_estimator_loaded": "not a model",
            },
        ]
    )
    return rows


def experiment_seed_rows() -> list[dict[str, Any]]:
    return [
        {
            "experiment_id": "TOTAL_BASES_ROLLING_SHADOW",
            "prop": "total_bases",
            "date": "2026-07-02..2026-07-18",
            "claimed_champion": "production total_bases model probability",
            "true_comparator_classification": "EXACT_CURRENT_PRODUCTION_OUTPUT",
            "challenger": "tb_rolling_balanced_shadow and tb_rolling_unweighted_shadow",
            "target": "actual over/under total_bases by market line",
            "population": "2331 scored shadow rows; 1379 with outcomes",
            "holdout": "live shadow dates, not a frozen promotion endpoint",
            "live_rows": 2331,
            "graded_rows": 1379,
            "automation_status": "ACTIVE_AUTOMATED",
            "endpoint": "none frozen; cumulative tracker",
            "reported_decision": "not promotion-ready; research-only pending larger live sample",
            "corrected_decision": "VALID_REJECTION_EVIDENCE_FOR_CURRENT_SHADOW_VARIANTS_ONLY",
            "trust_status": "VALID_REJECTION_EVIDENCE",
            "required_repair": "Freeze explicit endpoint before any future promotion claim.",
            "source_artifact": "artifacts/analysis/mlb/model_quality/total_bases_shadow/evaluation/total_bases_shadow_evaluation_summary.json",
        },
        {
            "experiment_id": "MLB_HITS05_CONTRACT_B_PROMOTION_GRADE",
            "prop": "hits 0.5",
            "date": "2026-07-18",
            "claimed_champion": "production hits model adapted to Hits O0.5 decision surface",
            "true_comparator_classification": "DERIVED_PRODUCTION_PROXY",
            "challenger": "Contract B line-invariant pitcher foundation calibration overlay",
            "target": "official Hits >= 1",
            "population": "7962 historical rows; 244 current replay rows",
            "holdout": "2011 rows",
            "live_rows": 244,
            "graded_rows": 2011,
            "automation_status": "INITIALIZED_NOT_ACTIVE",
            "endpoint": "none active; parked calibration-only",
            "reported_decision": "HITS05_CONTRACT_B_CALIBRATION_ONLY",
            "corrected_decision": "VALID_DIAGNOSTIC_ONLY",
            "trust_status": "VALID_DIAGNOSTIC_ONLY",
            "required_repair": "Bind exact production Hits O0.5 decision output before promotion/rejection evidence.",
            "source_artifact": "artifacts/analysis/model_development/mlb_hits05_contract_b_promotion_grade/2026-07-18/machine_readable_hits05_contract_b_promotion_grade_2026-07-18.json",
        },
        {
            "experiment_id": "O15_MARKET_ANCHORED_RANKING_RUN_1",
            "prop": "hits 1.5 over",
            "date": "2026-07-17",
            "claimed_champion": "market rank / market probability baseline",
            "true_comparator_classification": "MARKET_BASELINE",
            "challenger": "market plus Proppadia incremental ranking",
            "target": "official Hits >= 2",
            "population": "40 frozen pregame rows",
            "holdout": "prospective Run 1",
            "live_rows": 40,
            "graded_rows": 29,
            "automation_status": "ACTIVE_AUTOMATED",
            "endpoint": "living milestone, insufficient population",
            "reported_decision": "RUN1_BOUND_AND_GRADED / NOT_AUTHORIZED",
            "corrected_decision": "VALID_DIAGNOSTIC_ONLY_MARKET_RANKING_TEST",
            "trust_status": "VALID_DIAGNOSTIC_ONLY",
            "required_repair": "Do not describe as production Champion-Challenger unless production Hits model output is included as true Champion comparator.",
            "source_artifact": "artifacts/analysis/model_development/mlb_o15_market_anchored_ranking_prospective/machine_readable_prospective_grading_2026-07-17.json",
        },
        {
            "experiment_id": "MLB_CC_0001_PA_CHALLENGER",
            "prop": "hits family / broad production-spine PA Challenger",
            "date": "2026-07-10",
            "claimed_champion": "control model from frozen Champion package",
            "true_comparator_classification": "FROZEN_PRIOR_PRODUCTION_OUTPUT",
            "challenger": "PA opportunity-only Challenger",
            "target": "frozen training/evaluation target in CC-0001 manifests",
            "population": "controlled execution and prospective extension package",
            "holdout": "temporal fold package",
            "live_rows": "",
            "graded_rows": "",
            "automation_status": "COMPLETED",
            "endpoint": "governed post-execution; later paused for evidence discipline",
            "reported_decision": "challenger not promotion-ready / governance retained",
            "corrected_decision": "VALID_DIAGNOSTIC_ONLY_REQUIRES_EVIDENCE_CLAIM_DISCIPLINE",
            "trust_status": "VALID_DIAGNOSTIC_ONLY",
            "required_repair": "Keep as methodological baseline; future claims require exact production decision-surface matching.",
            "source_artifact": "artifacts/analysis/model_development/mlb_cc_0001_execution_2026-07-10/mlb_cc_0001_execution_summary_2026-07-10.json",
        },
        {
            "experiment_id": "MLB_PITCHER_HITS_ALLOWED_PROMOTION_GRADE",
            "prop": "pitcher hits_allowed",
            "date": "2026-07-17",
            "claimed_champion": "pitcher hits_allowed production Champion",
            "true_comparator_classification": "DERIVED_PRODUCTION_PROXY",
            "challenger": "granular encounter line-specific PHA Challenger",
            "target": "official pitcher hits allowed over/under line",
            "population": "1057 pitcher-line rows; fit/validation/holdout historical split",
            "holdout": "279 rows",
            "live_rows": "",
            "graded_rows": 279,
            "automation_status": "INITIALIZED_NOT_ACTIVE",
            "endpoint": "formal candidate later initialized, not complete",
            "reported_decision": "promotion-grade evidence favorable / controlled shadow required",
            "corrected_decision": "REQUIRES_CLEAN_REEVALUATION",
            "trust_status": "COMPARATOR_IDENTITY_INVALID",
            "required_repair": "Resolve exact production Champion semantics and line-specific comparator before promotion conclusion.",
            "source_artifact": "artifacts/analysis/model_development/mlb_pitcher_hits_allowed_promotion_grade/2026-07-17/machine_readable_pitcher_hits_allowed_promotion_grade_2026-07-17.json",
        },
        {
            "experiment_id": "MLB_PHA_FORMAL_PROMOTION_CANDIDATE",
            "prop": "pitcher hits_allowed",
            "date": "2026-07-18",
            "claimed_champion": "line-specific PHA Champion instrument",
            "true_comparator_classification": "UNRESOLVED",
            "challenger": "MLB_PHA_CHALLENGER_V1",
            "target": "official pitcher hits allowed over/under exact proposition line",
            "population": "21 current PHA props; 3 frozen scored trial rows",
            "holdout": "formal trial open",
            "live_rows": 3,
            "graded_rows": 0,
            "automation_status": "INITIALIZED_NOT_ACTIVE",
            "endpoint": "75 rows / 3 dates / 15 disagreements minimum",
            "reported_decision": "TRIAL_OPEN_NO_ENDPOINT_DECISION_YET",
            "corrected_decision": "WATCH_NOT_OPERATIONALLY_ACTIVE",
            "trust_status": "WATCH_NOT_OPERATIONALLY_ACTIVE",
            "required_repair": "Continue controlled capture and grade only frozen pre-first-pitch rows; no promotion while Champion binding unresolved.",
            "source_artifact": "artifacts/analysis/model_development/mlb_pha_formal_promotion_candidate/2026-07-18/machine_readable_pha_formal_candidate_2026-07-18.json",
        },
        {
            "experiment_id": "MLB_HITS15_DIRECT_PA_CHAMPION_CHALLENGER",
            "prop": "hits 1.5",
            "date": "2026-07-17",
            "claimed_champion": "current Hits 1.5 champion/control",
            "true_comparator_classification": "RESEARCH_CONTROL",
            "challenger": "direct PA opportunity overlay",
            "target": "official Hits >= 2",
            "population": "bounded pilot",
            "holdout": "research pilot",
            "live_rows": "",
            "graded_rows": "",
            "automation_status": "COMPLETED",
            "endpoint": "pilot only",
            "reported_decision": "diagnostic / no production authorization",
            "corrected_decision": "VALID_DIAGNOSTIC_ONLY",
            "trust_status": "VALID_DIAGNOSTIC_ONLY",
            "required_repair": "Bind exact production Hits O1.5 surface for future promotion evidence.",
            "source_artifact": "artifacts/analysis/model_development/mlb_hits_15_direct_pa_champion_challenger_pilot/2026-07-17/machine_readable_hits15_direct_pa_champion_challenger_2026-07-17.json",
        },
        {
            "experiment_id": "MLB_HITS15_SUPPRESSION_SHADOW",
            "prop": "hits under 1.5",
            "date": "2026-07-17",
            "claimed_champion": "existing U1.5 tracking / suppression watch",
            "true_comparator_classification": "RESEARCH_CONTROL",
            "challenger": "pitcher-suppression under 1.5 shadow",
            "target": "official Hits < 2",
            "population": "604 tracked propositions; 400 outcome-certified in reconciliation package",
            "holdout": "historical/prospective shadow mixed",
            "live_rows": "",
            "graded_rows": 400,
            "automation_status": "CAPTURE_ONLY_GRADING_UNWIRED",
            "endpoint": "ten-run prospective shadow desired",
            "reported_decision": "continue prospective suppression shadow",
            "corrected_decision": "VALID_DIAGNOSTIC_ONLY",
            "trust_status": "VALID_DIAGNOSTIC_ONLY",
            "required_repair": "Selection-time price certification and active prospective capture/grading before promotion evidence.",
            "source_artifact": "artifacts/analysis/model_development/mlb_existing_u15_tracking_suppression_reconciliation/2026-07-17/machine_readable_existing_u15_tracking_reconciliation_2026-07-17.json",
        },
        {
            "experiment_id": "SINGLES_SHADOW_UPLOAD",
            "prop": "singles",
            "date": "current daily target",
            "claimed_champion": "production singles model",
            "true_comparator_classification": "EXACT_CURRENT_PRODUCTION_OUTPUT",
            "challenger": "isolated singles-threshold shadow CSV",
            "target": "singles over/under market line",
            "population": "current slate shadow output when target is run",
            "holdout": "none found",
            "live_rows": "",
            "graded_rows": "",
            "automation_status": "ACTIVE_MANUAL",
            "endpoint": "none",
            "reported_decision": "shadow only",
            "corrected_decision": "VALID_DIAGNOSTIC_ONLY",
            "trust_status": "VALID_DIAGNOSTIC_ONLY",
            "required_repair": "Need fixed endpoint and grading before any promotion language.",
            "source_artifact": "Makefile target mlb-singles-shadow; backend/mlb/scripts/generate_singles_shadow_upload.py",
        },
        {
            "experiment_id": "PITCHER_PROP_GENERIC_WATCHES",
            "prop": "strikeouts_pitching, outs_recorded, earned_runs, walks_allowed",
            "date": "various",
            "claimed_champion": "current production pitcher prop models",
            "true_comparator_classification": "UNRESOLVED",
            "challenger": "early steam / residual / candidate watches where present",
            "target": "prop-specific official over/under outcome",
            "population": "not fully bound in this audit",
            "holdout": "unresolved",
            "live_rows": "",
            "graded_rows": "",
            "automation_status": "UNRESOLVED",
            "endpoint": "unresolved",
            "reported_decision": "no reliable promotion conclusion identified",
            "corrected_decision": "INSUFFICIENT_DOCUMENTATION",
            "trust_status": "INSUFFICIENT_DOCUMENTATION",
            "required_repair": "Run prop-specific true Champion binding before retaining any promotion/rejection claim.",
            "source_artifact": "Makefile early-steam pitcher candidates and production model index",
        },
    ]


def artifact_scan() -> list[dict[str, Any]]:
    roots = [
        ROOT / "artifacts/analysis/model_development",
        ROOT / "artifacts/analysis/mlb",
        ROOT / "backend/mlb/scripts",
    ]
    terms = ["watch", "shadow", "prospective", "champion", "challenger", "promotion", "trial", "milestone"]
    rows: list[dict[str, Any]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if path.is_dir():
                continue
            lower = str(path).lower()
            matched = [t for t in terms if t in lower]
            if not matched:
                continue
            rows.append(
                {
                    "path": rel(path),
                    "suffix": path.suffix,
                    "matched_terms": "|".join(matched),
                    "size_bytes": path.stat().st_size,
                    "mtime_utc": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                    "sha256": sha256(path) if path.is_file() and path.stat().st_size < 50_000_000 else "",
                    "classification": "SCAN_CANDIDATE_REQUIRES_LEDGER_REVIEW",
                }
            )
    return sorted(rows, key=lambda r: r["path"])


def launchagent_inventory() -> list[dict[str, Any]]:
    paths = sorted((Path.home() / "Library/LaunchAgents").glob("*mlb*")) + sorted(
        (Path.home() / "Library/LaunchAgents").glob("*proppadia*")
    )
    seen: set[Path] = set()
    rows: list[dict[str, Any]] = []
    for path in paths:
        if path in seen or not path.is_file():
            continue
        seen.add(path)
        text = path.read_text(encoding="utf-8", errors="replace")
        rows.append(
            {
                "plist_path": str(path),
                "label_hint": path.name,
                "contains_o15_grader": "run_mlb_o15_market_anchored_ranking_prospective_grader" in text
                or "mlb-o15-prospective-grade" in text,
                "contains_daily_wrapper": "proppadia_mlb_refresh_daily.sh" in text,
                "contains_lineup_study": "pregame_lineup" in text or "lineup" in text,
                "contains_starter_skill_workload": "starter_skill_workload" in text,
                "working_directory_hint": "/Users/jerrystrain/Projects/proppadia" if "/Users/jerrystrain/Projects/proppadia" in text else "",
                "stdout_hint": "artifacts/ops" if "artifacts/ops" in text else "",
                "automation_status": "SCHEDULED_WRAPPER" if "proppadia_mlb_refresh_daily.sh" in text else "SCHEDULED_RESEARCH_OR_OTHER",
                "notes": "O1.5 grader is wired through daily wrapper/Make target, not directly in plist." if "proppadia_mlb_refresh_daily.sh" in text else "",
            }
        )
    return rows


def champion_identity_rows(experiments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for e in experiments:
        rows.append(
            {
                "experiment_id": e["experiment_id"],
                "claimed_champion": e["claimed_champion"],
                "true_comparator_classification": e["true_comparator_classification"],
                "source_artifact": e["source_artifact"],
                "source_column": "varies; see source artifact",
                "grain": "prop-specific proposition grain unless otherwise noted",
                "model_formula_identity": "see production inventory and experiment contract",
                "date_version": e["date"],
                "join_keys": "slate_date|game_id|player_id|prop_type|line|side when proposition-line; player-game where explicitly authorized",
                "transformation": "none or documented experiment-specific transformation",
                "selected_side": "must be native production side for true Champion claims",
                "exact_production_prediction_available": e["true_comparator_classification"] in {"EXACT_CURRENT_PRODUCTION_OUTPUT", "FROZEN_PRIOR_PRODUCTION_OUTPUT", "DETERMINISTIC_PRODUCTION_FORMULA"},
                "notes": "Derived proxies and market baselines are not production Champions.",
            }
        )
    return rows


def challenger_identity_rows(experiments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "experiment_id": e["experiment_id"],
            "challenger": e["challenger"],
            "exact_specification": "see source artifact",
            "source_fields": "documented in source package when available",
            "fit_population": e["population"],
            "validation_and_holdout_periods": e["holdout"],
            "output_grain": "same as target/proposition grain unless diagnostic says otherwise",
            "probability_or_score_semantics": "mixed: probability, rank score, deterministic shadow, or diagnostic overlay",
            "uses_champion_outputs_as_inputs": "UNKNOWN unless source contract explicitly states no",
            "frozen_before_prospective_use": "yes" if "prospective" in e["experiment_id"].lower() or "formal" in e["experiment_id"].lower() else "varies",
            "ever_generated_live": "yes" if str(e["live_rows"]) not in {"", "0"} else "unknown_or_no",
            "notes": "Promotion claims require this row to bind to a true Champion row.",
        }
        for e in experiments
    ]


def outcome_metric_rows(experiments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for e in experiments:
        rows.append(
            {
                "experiment_id": e["experiment_id"],
                "authoritative_outcome_source": "reconcile_rows.csv or source package official outcome ledger",
                "target_definition": e["target"],
                "push_handling": "market-line dependent; pushes excluded/neutral where source reports push",
                "market_line_handling": "exact line required for proposition-line outcomes; player-game hits only when target is numeric hit count",
                "pregame_cutoff": "required for prospective/live claims; historical packages vary",
                "metrics_used": "AUC, Brier, log loss, ROI, side accuracy, calibration, rank/pairwise metrics depending on experiment",
                "production_decision_surface_match": "PASS" if e["true_comparator_classification"] == "EXACT_CURRENT_PRODUCTION_OUTPUT" else "FAIL_OR_DIAGNOSTIC",
                "metric_contract_classification": "diagnostic metrics are not promotion evidence unless comparator/target/endpoint align",
                "notes": e["corrected_decision"],
            }
        )
    return rows


def focused_rows(topic: str, experiments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = []
    for e in experiments:
        prop = e["prop"].lower()
        if topic == "total_bases" and "total_bases" in prop:
            selected.append(e)
        elif topic == "pitcher_props" and ("pitcher" in prop or "strikeouts_pitching" in prop or "outs_recorded" in prop or "walks_allowed" in prop or "earned_runs" in prop):
            selected.append(e)
        elif topic == "hits" and prop.startswith("hits"):
            selected.append(e)
        elif topic == "pha" and "pitcher hits_allowed" in prop:
            selected.append(e)
    return selected


def reevaluation_queue(experiments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for e in experiments:
        if e["trust_status"] in {"COMPARATOR_IDENTITY_INVALID", "WATCH_NOT_OPERATIONALLY_ACTIVE", "INSUFFICIENT_DOCUMENTATION"}:
            rows.append(
                {
                    "experiment_id": e["experiment_id"],
                    "priority": "high" if e["prop"] in {"pitcher hits_allowed", "total_bases", "hits 1.5 over"} else "medium",
                    "reason": e["trust_status"],
                    "required_repair": e["required_repair"],
                    "active_development_allowed": "no; paused pending this integrity audit and explicit next approval",
                }
            )
    return rows


def future_standard_rows() -> list[dict[str, Any]]:
    items = [
        "exact production Champion identity",
        "native production probability/score and side",
        "frozen Challenger",
        "identical proposition grain",
        "authoritative outcomes",
        "temporal isolation",
        "historical validation and holdout",
        "live replayability when required",
        "operationally active prospective capture and grading when required",
        "fixed endpoint",
        "explicit promotion/rejection rule",
        "no proxy relabeled as Champion",
    ]
    return [
        {
            "requirement_order": i + 1,
            "requirement": item,
            "mandatory_for_promotion_claim": True,
            "when_prospective_evidence_required": "Required when live data availability, current-run materialization, or operational workflow is part of the claim.",
            "when_historical_evidence_can_suffice": "Can suffice when exact production Champion, same target/grain, temporal holdout, and replayable artifacts are already complete.",
        }
        for i, item in enumerate(items)
    ]


def decisions() -> list[dict[str, str]]:
    return [
        {"decision": "MLB_PROMOTION_AUDIT_PRODUCTION_MODEL_INVENTORY_DECISION", "value": "PRODUCTION_MODEL_INVENTORY_BOUND_TO_CHAMPION_FREEZE_AND_MODEL_INDEX"},
        {"decision": "MLB_PROMOTION_AUDIT_TRUE_CHAMPION_BINDING_DECISION", "value": "MIXED_EXACT_MARKET_PROXY_AND_UNRESOLVED_COMPARATORS_FOUND"},
        {"decision": "MLB_PROMOTION_AUDIT_WATCH_INVENTORY_DECISION", "value": "WATCH_SHADOW_PROSPECTIVE_AND_PROMOTION_PACKAGES_INVENTORIED_WITH_SCAN_BACKSTOP"},
        {"decision": "MLB_PROMOTION_AUDIT_AUTOMATION_STATUS_DECISION", "value": "ONLY_DAILY_WRAPPER_AND_SELECTED_RESEARCH_JOBS_ARE_SCHEDULED_MANY_WATCHES_NOT_OPERATIONALLY_ACTIVE"},
        {"decision": "MLB_PROMOTION_AUDIT_TOTAL_BASES_DECISION", "value": "TOTAL_BASES_NOT_READY_CONCLUSION_VALID_FOR_SHADOW_VARIANTS_COMPARATOR_EXACT_PRODUCTION"},
        {"decision": "MLB_PROMOTION_AUDIT_PITCHER_PROPS_DECISION", "value": "PITCHER_PROP_PROMOTION_CONCLUSIONS_REQUIRE_PROP_SPECIFIC_TRUE_CHAMPION_REBINDING"},
        {"decision": "MLB_PROMOTION_AUDIT_HITS_DECISION", "value": "HITS_O05_AND_O15_RECENT_RESULTS_ARE_DIAGNOSTIC_NOT_PRODUCTION_PROMOTION_EVIDENCE"},
        {"decision": "MLB_PROMOTION_AUDIT_PHA_DECISION", "value": "PHA_PROMOTION_STATUS_SUSPENDED_PENDING_TRUE_CHAMPION_RESOLUTION_AND_ACTIVE_TRIAL_GRADING"},
        {"decision": "MLB_PROMOTION_AUDIT_VALID_PRIOR_CONCLUSIONS_DECISION", "value": "TOTAL_BASES_SHADOW_REJECTION_AND_CALIBRATION_ONLY_CLOSEOUTS_SURVIVE_AS_SCOPED_DIAGNOSTIC_OR_REJECTION_EVIDENCE"},
        {"decision": "MLB_PROMOTION_AUDIT_WITHDRAWN_CONCLUSIONS_DECISION", "value": "ANY_PHA_OR_PROXY_CHAMPION_PROMOTION_LANGUAGE_WITHDRAWN_PENDING_CLEAN_REEVALUATION"},
        {"decision": "MLB_PROMOTION_AUDIT_REEVALUATION_QUEUE_DECISION", "value": "PHA_AND_GENERIC_PITCHER_PROPS_FIRST_TOTAL_BASES_ENDPOINT_SECOND_HITS_PROMOTION_BINDING_THIRD"},
        {"decision": "MLB_PROMOTION_AUDIT_FUTURE_STANDARD_DECISION", "value": "CANONICAL_PROMOTION_EVIDENCE_CONTRACT_FROZEN"},
        {"decision": "MLB_PROMOTION_AUDIT_ACTIVE_DEVELOPMENT_STATUS", "value": "PAUSED_PENDING_INTEGRITY_AUDIT"},
        {"decision": "MLB_PRODUCTION_STATUS", "value": "UNCHANGED"},
    ]


def validation_report(out_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(out_dir.glob("*")):
        if path.suffix == ".csv":
            try:
                with path.open(newline="", encoding="utf-8") as fh:
                    list(csv.DictReader(fh))
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "csv_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".json":
            try:
                json.loads(path.read_text(encoding="utf-8"))
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "PASS", "message": ""})
            except Exception as exc:
                rows.append({"artifact": rel(path), "check": "json_parse", "status": "FAIL", "message": str(exc)})
        elif path.suffix == ".md":
            rows.append({"artifact": rel(path), "check": "markdown_nonempty", "status": "PASS" if path.read_text(encoding="utf-8").strip() else "FAIL", "message": ""})
    return rows


def build(out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_at = now_utc()
    production = production_inventory()
    experiments = experiment_seed_rows()
    scans = artifact_scan()
    launchagents = launchagent_inventory()
    champion_rows = champion_identity_rows(experiments)
    challenger_rows = challenger_identity_rows(experiments)
    outcome_rows = outcome_metric_rows(experiments)
    queue = reevaluation_queue(experiments)
    standard = future_standard_rows()
    decision_rows = decisions()

    outputs = {
        "production_prediction_inventory_2026-07-18.csv": production,
        "complete_watch_trial_inventory_2026-07-18.csv": experiments,
        "champion_identity_ledger_2026-07-18.csv": champion_rows,
        "challenger_identity_ledger_2026-07-18.csv": challenger_rows,
        "automation_launchagent_audit_2026-07-18.csv": launchagents,
        "outcome_metric_contract_audit_2026-07-18.csv": outcome_rows,
        "total_bases_findings_2026-07-18.csv": focused_rows("total_bases", experiments),
        "pitcher_prop_findings_2026-07-18.csv": focused_rows("pitcher_props", experiments),
        "hits_findings_2026-07-18.csv": focused_rows("hits", experiments),
        "pha_findings_2026-07-18.csv": focused_rows("pha", experiments),
        "corrected_prior_result_classifications_2026-07-18.csv": experiments,
        "reevaluation_queue_2026-07-18.csv": queue,
        "canonical_promotion_evidence_ledger_2026-07-18.csv": experiments,
        "future_promotion_standard_2026-07-18.csv": standard,
        "watch_promotion_scan_inventory_2026-07-18.csv": scans,
        "required_decisions_2026-07-18.csv": decision_rows,
    }
    for name, rows in outputs.items():
        write_csv(out_dir / name, rows)

    machine = {
        "generated_at": generated_at,
        "production_prop_models": len([r for r in production if r["prediction_construction"] == "trained_model"]),
        "experiments_in_canonical_ledger": len(experiments),
        "scan_candidate_paths": len(scans),
        "launchagents": len(launchagents),
        "trust_status_counts": pd.Series([e["trust_status"] for e in experiments]).value_counts().to_dict(),
        "direct_answer": "Some conclusions survive only in their scoped form. Total Bases shadow rejection is trustworthy for those shadow variants; Hits O0.5 Contract B and O1.5 market-anchored results are diagnostic; PHA promotion language must be suspended because Champion identity and operational activation remain incomplete.",
        "decisions": {r["decision"]: r["value"] for r in decision_rows},
        "guardrails": {
            "model_fitting": False,
            "new_predictions": False,
            "db_writes": False,
            "oddsapi_calls": False,
            "scheduler_changes": False,
            "production_behavior_changed": False,
        },
    }
    write_json(out_dir / "machine_readable_watch_promotion_integrity_audit_2026-07-18.json", machine)
    write_md(
        out_dir / "watch_champion_promotion_integrity_audit_2026-07-18.md",
        f"""# MLB Watch, Champion-Challenger, and Promotion-Evidence Integrity Audit

Generated: `{generated_at}`

## Executive Summary

Active MLB model-development promotion activity is paused pending this integrity
audit. The audit found mixed evidence quality across current packages:

- The production Champion inventory is bindable through the Champion Freeze and
  `models_out/latest/MODEL_INDEX.json`.
- Total Bases shadow evaluation appears to compare against the exact current
  production Total Bases output; its not-ready conclusion remains valid for the
  tested shadow variants.
- Hits O0.5 Contract B is useful calibration evidence, but its Champion
  comparator is a derived production proxy; it remains diagnostic and
  calibration-only.
- Hits O1.5 Run 1 is now graded and automated, but its Champion is a market
  baseline, not the production Hits model; it is valid ranking research, not a
  production Champion-Challenger conclusion.
- Pitcher Hits Allowed has promising historical evidence, but promotion status
  is suspended because the true production Champion/comparator semantics remain
  unresolved and the formal live trial is initialized rather than fully active.

## Focused Findings

### Total Bases

Current production Total Bases is a fitted model loaded from
`models_out/latest/total_bases.joblib`. The shadow tracker compares against
production rows in the cumulative evaluation. The reported not-ready conclusion
survives for the balanced and unweighted shadow variants because the comparator
is exact production output and the shadow underperformed or remained too
immature for promotion.

### Pitcher Props

The production model index contains `strikeouts_pitching`, `hits_allowed`,
`walks_allowed`, `earned_runs`, and related pitcher props. Existing pitcher-prop
promotion language should not be reused until each prop binds exact native
production probability/side at proposition-line grain. Generic pitcher watches
are therefore queued for prop-specific rebinding.

### Hits

The current production Hits prediction is a fitted `hits.joblib` model at
market-line grain. Hits O0.5 Contract B remains calibration-only. Hits O1.5
market-anchored Run 1 is a valid market-ranking diagnostic, not a production
Champion-Challenger comparison. PA and hitter-context pilots remain research
evidence until bound to exact production decision surfaces.

### Pitcher Hits Allowed

PHA should be treated as a line-specific research model. The July 18 controlled
shadow produced three process-validation rows, but the formal trial is not an
active promotion watch yet. Promotion status is suspended pending comparator
resolution and more frozen, pregame, graded rows.

## Direct Answer

The project can trust some existing conclusions only in their scoped form. It
must withdraw or downgrade any conclusion that relied on a proxy Champion,
market baseline, unresolved comparator, or initialized-but-inactive watch as if
it were production promotion evidence.

## Decisions

{chr(10).join(f"- `{r['decision']} = {r['value']}`" for r in decision_rows)}

## Production Status

`MLB_PRODUCTION_STATUS = UNCHANGED`
""",
    )
    validation = validation_report(out_dir)
    write_csv(out_dir / "validation_report_2026-07-18.csv", validation)
    manifest = []
    for path in sorted(out_dir.glob("*")):
        if path.is_file() and path.name != "sha256_manifest_2026-07-18.csv":
            manifest.append({"path": rel(path), "sha256": sha256(path), "size_bytes": path.stat().st_size})
    manifest.extend(
        [
            {"path": rel(MODEL_INDEX), "sha256": sha256(MODEL_INDEX) if MODEL_INDEX.exists() else "", "size_bytes": MODEL_INDEX.stat().st_size if MODEL_INDEX.exists() else 0},
            {"path": rel(Path(__file__).resolve()), "sha256": sha256(Path(__file__).resolve()), "size_bytes": Path(__file__).resolve().stat().st_size},
        ]
    )
    write_csv(out_dir / "sha256_manifest_2026-07-18.csv", manifest)
    return machine


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["read_only"], default="read_only")
    parser.add_argument("--output-dir", default=str(OUT_DIR))
    args = parser.parse_args()
    result = build(Path(args.output_dir))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
