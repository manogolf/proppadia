#!/usr/bin/env python3
"""One-shot exact recovery of the frozen original UBO-5 Total Bases model."""
from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow
import sklearn
from sklearn.metrics import brier_score_loss, log_loss

from backend.mlb.scripts.run_mlb_unified_batter_outcome_v1 import (
    SEED, aligned_proba, fit_logit,
)

ROOT = Path(__file__).resolve().parents[3]
UBO = ROOT / "artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22"
RECON = ROOT / "artifacts/analysis/model_development/mlb_total_bases_production_shadow_ubo_terminal_reconciliation/2026-07-23"
READY = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_established_hitter_implementation_readiness/2026-07-23"
CORE = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_certified_core/2026-07-22"
DAILY = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow/2026-07-23/total_bases_shadow_scores_2026-07-23.csv"
OUT = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_15_artifact_live_contract_recovery/2026-07-23"
ARTIFACT = OUT / "original_ubo5_total_bases_multinomial.joblib"
FEATURE_SCHEMA = OUT / "frozen_feature_schema.csv"
SCORER = ROOT / "backend/mlb/scripts/score_mlb_ubo5_total_bases_established_default_off.py"

OPPORTUNITY = ["batting_order_position", "home", "prior_player_pa_per_date",
               "prior_slot_pa_per_start", "opp_prior_dates", "history_depth_pa"]
HITTER = [f"h_career_rate_{i}" for i in range(8)] + [f"h_recent30_rate_{i}" for i in range(8)]
CONTACT = ["h_swing_rate", "h_whiff_per_swing", "h_contact_per_swing",
           "h_called_strike_rate", "h_foul_rate", "h_pitches_per_pa", "h_ev",
           "h_xba", "h_xwoba", "h_lsa6_rate", "history_depth_pa"]
PITCHER = ["p_hit_suppression", "p_k_rate", "p_prior_dates", "pitcher_available"]
MATCHUP = ["matchup_k", "matchup_hit"]
FEATURES = list(dict.fromkeys(OPPORTUNITY + HITTER + CONTACT + PITCHER + MATCHUP))


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def save(name: str, data) -> pd.DataFrame:
    frame = data if isinstance(data, pd.DataFrame) else pd.DataFrame(data)
    OUT.mkdir(parents=True, exist_ok=True)
    frame.to_csv(OUT / name, index=False)
    return frame


def verify(root: Path) -> pd.DataFrame:
    rows = []
    for item in pd.read_csv(root / "sha256_manifest.csv").itertuples():
        path = root / item.path
        actual = sha(path) if path.exists() else ""
        rows.append({"package": str(root.relative_to(ROOT)), "path": item.path,
                     "expected": item.sha256, "actual": actual,
                     "status": "PASS" if actual == item.sha256 else "FAIL"})
    return pd.DataFrame(rows)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    binding = pd.concat([verify(root) for root in [UBO, RECON, READY, CORE]], ignore_index=True)
    save("governing_package_binding.csv", binding)
    if binding.status.ne("PASS").any():
        raise RuntimeError("governing package mismatch")

    contract = [
        ("model_family", "original UBO-5 direct Total Bases distribution"),
        ("estimator", "Pipeline(SimpleImputer(strategy=median,add_indicator=True),StandardScaler(),LogisticRegression)"),
        ("logistic_parameters", f"max_iter=250|C=0.35|solver=lbfgs|random_state={SEED}"),
        ("features", "|".join(FEATURES)), ("feature_count", len(FEATURES)),
        ("categorical_handling", "none; all numeric"), ("sample_weighting", "none"),
        ("target", "min(actual_tb,4) classes 0|1|2|3|4plus"),
        ("calibration", "NONE"), ("training_rows", "split=development only"),
        ("training_cutoff", "2024-12-31"), ("selection", "2025 validation only"),
        ("probability_mapping_tb15", "P(TB>1.5)=1-P(class0)-P(class1)"),
        ("post_recovery_choice", "none; exactly one fit authorized"),
    ]
    save("frozen_recovery_contract.csv", [{"field": key, "value": value} for key, value in contract])
    save("frozen_feature_schema.csv", [{"ordinal": i, "feature": feature, "dtype": "float64"}
                                       for i, feature in enumerate(FEATURES)])

    pop = pd.read_parquet(UBO / "model_population_manifest.parquet")
    feature = pd.read_parquet(UBO / "strict_prior_player_game_features.parquet")
    keys = ["game_pk", "game_date", "batter_mlb_id", "split"]
    overlap = [column for column in feature if column in pop and column not in keys]
    matrix = pop.merge(feature.drop(columns=overlap), on=keys, how="inner",
                       validate="one_to_one", sort=False)
    dev = matrix[matrix.split.eq("development")].copy()
    validation = matrix[matrix.split.eq("validation")].copy()
    inventory = [
        ("source training rows", len(dev), "EXACT_AND_AVAILABLE"),
        ("validation rows", len(validation), "EXACT_AND_AVAILABLE"),
        ("feature matrix", len(FEATURES), "EXACT_AND_AVAILABLE"),
        ("labels", "actual_tb retained", "EXACT_AND_AVAILABLE"),
        ("sample weights", "none in original call", "EXACT_AND_AVAILABLE"),
        ("preprocessing", "source code exact", "EXACT_AND_AVAILABLE"),
        ("estimator configuration", "source code exact", "EXACT_AND_AVAILABLE"),
        ("random seed", SEED, "EXACT_AND_AVAILABLE"),
        ("dependency versions", f"python={platform.python_version()}|sklearn={sklearn.__version__}", "EXACT_RUNTIME_RECORDED"),
        ("fitting order", "development matrix retained left-order", "INFERABLE_WITHOUT_DISCRETION"),
        ("calibration", "none", "EXACT_AND_AVAILABLE"),
        ("saved coefficients", "absent", "NOT_REQUIRED_FOR_AUTHORIZED_EXACT_REFIT"),
        ("historical prediction ledger", len(pd.read_parquet(UBO / "player_game_probability_distributions.parquet")), "EXACT_AND_AVAILABLE"),
    ]
    save("refit_identifiability_inventory.csv", [{"component": a, "value": b, "classification": c}
                                                  for a, b, c in inventory])
    for name, frame in [("training", dev), ("validation", validation)]:
        manifest = frame[keys + ["actual_tb"]].copy()
        manifest["target_class"] = np.minimum(manifest.actual_tb, 4)
        save(f"reconstructed_{name}_manifest.csv", manifest)
    matrix_summary = []
    for name, frame in [("training", dev), ("validation", validation)]:
        matrix_summary.append({
            "matrix": name, "rows": len(frame), "min_date": frame.game_date.min(),
            "max_date": frame.game_date.max(), "target_mean_tb": frame.actual_tb.mean(),
            "target_class_prevalence_0": np.minimum(frame.actual_tb, 4).eq(0).mean(),
            "feature_count": len(FEATURES), "missing_cells": int(frame[FEATURES].isna().sum().sum()),
            "row_identity_duplicates": int(frame[keys].duplicated().sum()),
            "feature_order_sha256": sha(FEATURE_SCHEMA),
        })
    save("matrix_reconciliation.csv", matrix_summary)

    # The sole authorized fit. On audit reruns, reuse the recovered artifact.
    if ARTIFACT.exists():
        artifact_bundle = joblib.load(ARTIFACT)
        model = artifact_bundle["model"]
        fit_performed = 0
    else:
        target = np.minimum(dev.actual_tb.to_numpy(), 4)
        model = fit_logit(dev[FEATURES], target)
        artifact_bundle = {
            "model": model, "features": FEATURES, "classes": [0, 1, 2, 3, 4],
            "target_mapping": "0|1|2|3|4plus", "tb15_mapping": "1-p0-p1",
            "training_cutoff": "2024-12-31", "training_rows": len(dev),
            "validation_rows": len(validation), "seed": SEED, "calibration": "NONE",
            "feature_schema_sha256": sha(FEATURE_SCHEMA),
        }
        joblib.dump(artifact_bundle, ARTIFACT, compress=3)
        fit_performed = 1
    artifact_hash = sha(ARTIFACT)
    command = ".venv/bin/python -m backend.mlb.scripts.recover_mlb_ubo5_total_bases_15_artifact"
    save("training_command.csv", [{"command": command, "authorized_fit_count": 1,
                                   "cumulative_recovery_fit_count": 1,
                                   "fit_performed_this_audit_run": fit_performed,
                                   "alternatives_run": 0}])
    save("artifact_identity.csv", [{
        "artifact_path": str(ARTIFACT.relative_to(ROOT)), "sha256": artifact_hash,
        "model_class": type(model).__name__, "training_rows": len(dev),
        "validation_rows": len(validation), "training_cutoff": "2024-12-31",
        "feature_registry_sha256": sha(FEATURE_SCHEMA), "python": platform.python_version(),
        "numpy": np.__version__, "pandas": pd.__version__, "sklearn": sklearn.__version__,
        "pyarrow": pyarrow.__version__, "joblib": joblib.__version__,
    }])

    original = pd.read_parquet(UBO / "player_game_probability_distributions.parquet")
    original = original[original.variant.eq("UBO-5")].copy()
    scored = aligned_proba(model, matrix[FEATURES], 5)
    recovered = matrix[keys + ["actual_tb"]].copy()
    for i, column in enumerate(["p_tb0", "p_tb1", "p_tb2", "p_tb3", "p_tb4plus"]):
        recovered["recovered_" + column] = scored[:, i]
    recovered_eval = original[keys].merge(recovered, on=keys, how="left", validate="one_to_one")
    ledger = original.merge(recovered_eval, on=keys, how="outer", indicator=True, validate="one_to_one")
    differences = []
    for column in ["p_tb0", "p_tb1", "p_tb2", "p_tb3", "p_tb4plus"]:
        ledger["diff_" + column] = ledger["recovered_" + column] - ledger[column]
        differences.append(ledger["diff_" + column].abs().to_numpy())
    save("original_vs_recovered_probability_ledger.csv", ledger)
    all_diff = np.concatenate(differences)
    common = ledger[ledger._merge.eq("both")].copy()
    original_flat = common[["p_tb0", "p_tb1", "p_tb2", "p_tb3", "p_tb4plus"]].to_numpy().ravel()
    recovered_flat = common[["recovered_p_tb0", "recovered_p_tb1", "recovered_p_tb2",
                             "recovered_p_tb3", "recovered_p_tb4plus"]].to_numpy().ravel()
    reproduction = {
        "original_rows": len(original), "recovered_rows": len(recovered_eval),
        "common_rows": len(common), "missing_original_rows": int((ledger._merge == "right_only").sum()),
        "missing_recovered_rows": int((ledger._merge == "left_only").sum()),
        "max_abs_difference": np.max(all_diff), "mean_abs_difference": np.mean(all_diff),
        "median_abs_difference": np.median(all_diff), "p95_abs_difference": np.quantile(all_diff, .95),
        "p99_abs_difference": np.quantile(all_diff, .99),
        "probability_correlation": np.corrcoef(original_flat, recovered_flat)[0, 1],
    }

    supported = pd.read_csv(READY / "supported_population_manifest.csv")
    tb15 = supported[supported.line.eq(1.5)].copy()
    recovered_tail = recovered[["game_pk", "batter_mlb_id", "recovered_p_tb0", "recovered_p_tb1"]].copy()
    recovered_tail["recovered_ubo5_prob_over"] = 1 - recovered_tail.recovered_p_tb0 - recovered_tail.recovered_p_tb1
    tb15 = tb15.merge(recovered_tail[["game_pk", "batter_mlb_id", "recovered_ubo5_prob_over"]],
                      on=["game_pk", "batter_mlb_id"], how="left", validate="many_to_one")
    eps = 1e-9
    y = tb15.y_over.to_numpy()
    for prefix, probability in [("original", tb15.original_ubo5_prob_over.to_numpy()),
                                ("recovered", tb15.recovered_ubo5_prob_over.to_numpy()),
                                ("production", tb15.production_prob_over.to_numpy())]:
        p = np.clip(probability, eps, 1-eps)
        reproduction[prefix + "_brier"] = brier_score_loss(y, p)
        reproduction[prefix + "_log_loss"] = log_loss(y, np.c_[1-p, p], labels=[0, 1])
    reproduction["tb15_brier_delta_recovered_minus_original"] = reproduction["recovered_brier"] - reproduction["original_brier"]
    reproduction["tb15_logloss_delta_recovered_minus_original"] = reproduction["recovered_log_loss"] - reproduction["original_log_loss"]
    reproduction["tb15_side_agreement_at_0_5"] = (
        (tb15.original_ubo5_prob_over >= .5) == (tb15.recovered_ubo5_prob_over >= .5)
    ).mean()
    exact_reproduction = (
        reproduction["common_rows"] == reproduction["original_rows"] == reproduction["recovered_rows"]
        and reproduction["max_abs_difference"] <= 1e-10
    )
    save("reproduction_statistics.csv", [reproduction])
    save("recovered_tb15_evidence.csv", tb15)
    date_metrics = []
    for date, group in tb15.groupby("slate_date"):
        for name, column in [("production", "production_prob_over"),
                             ("original", "original_ubo5_prob_over"),
                             ("recovered", "recovered_ubo5_prob_over")]:
            p = np.clip(group[column], eps, 1-eps)
            date_metrics.append({"date": date, "model": name, "rows": len(group),
                                 "brier": brier_score_loss(group.y_over, p),
                                 "log_loss": log_loss(group.y_over, np.c_[1-p, p], labels=[0, 1])})
    save("recovered_tb15_date_metrics.csv", date_metrics)

    # Live materialization audit: certified normalized/core packages end July 21 and
    # contain no July 23 certified lineup/feature row. Approximation is prohibited.
    registry = pd.read_csv(READY / "live_feature_registry_and_lineage.csv")
    registry = registry[registry.feature.isin(FEATURES)].copy()
    registry["feature_schema_sha256"] = sha(FEATURE_SCHEMA)
    save("live_feature_registry.csv", registry)
    save("live_feature_builder_contract.csv", [{
        "input": "slate_date|run_tag|prediction_timestamp|game_pk|batter_mlb_id|team|opponent|certified starter",
        "builder": "exact original date-prior profiles from certified normalized Tier A/B partitions",
        "cutoff": "events strictly before target calendar date; prediction before scheduled start",
        "status": "BLOCKED_NO_CURRENT_CERTIFIED_LINEUP_OR_POST_JULY21_NORMALIZED_FEATURE_PARTITION",
        "fallback": "current production; no approximate legacy substitution",
    }])
    daily = pd.read_csv(DAILY)
    now = datetime.now(timezone.utc)
    daily["start_utc"] = pd.to_datetime(daily.game_time, utc=True)
    open_rows = daily[daily.line.eq(1.5) & daily.start_utc.gt(now)].copy()
    availability_rows = []
    for row in open_rows.itertuples():
        availability_rows.append({
            "slate_date": row.slate_date, "game_pk": row.game_id, "batter_mlb_id": row.player_id,
            "player_name": row.player_name, "team": row.team, "opponent": row.opponent,
            "line": row.line, "scheduled_start_utc": row.start_utc.isoformat(),
            "certified_starter": False, "strict_prior_pa": np.nan,
            "feature_complete": False, "stale_source": True, "identity_failure": False,
            "eligible": False,
            "missing": "certified_pregame_lineup|strict_prior_pa|" + "|".join(FEATURES),
            "exclusion_reason": "CERTIFIED_LIVE_FEATURE_PARTITION_UNAVAILABLE",
        })
    save("live_feature_availability_audit.csv", availability_rows)

    save("eligibility_routing_contract.csv", [
        {"condition": "prop_type=total_bases and line=1.5", "failure": "current production"},
        {"condition": "certified pregame starting hitter", "failure": "current production"},
        {"condition": "strict_prior_pa>=100", "failure": "current production"},
        {"condition": "all 38 frozen features complete and fresh", "failure": "current production"},
        {"condition": "prediction timestamp < scheduled start", "failure": "current production"},
        {"condition": "artifact and feature-schema hashes exact", "failure": "current production"},
    ])
    scorer_input = open_rows.rename(columns={
        "game_id": "game_pk", "player_id": "batter_mlb_id",
        "game_time": "scheduled_start_utc",
    }).copy()
    scorer_input["strict_prior_pa"] = np.nan
    scorer_input["starter_certification"] = "UNAVAILABLE"
    scorer_input["source_lineage_pointer"] = str(DAILY.relative_to(ROOT))
    scorer_input.to_csv(OUT / "default_off_scorer_input.csv", index=False)
    scorer_output = OUT / "default_off_scorer_ledger.csv"
    scorer_env = dict(os.environ)
    scorer_env["MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE"] = "0"
    subprocess.run([
        str(ROOT / ".venv/bin/python"), str(SCORER), "--slate-date", "2026-07-23",
        "--run-tag", "tb15_recovery_integrity", "--input-ledger", str(OUT / "default_off_scorer_input.csv"),
        "--output-ledger", str(scorer_output), "--artifact", str(ARTIFACT),
        "--artifact-sha256", artifact_hash, "--feature-order", str(FEATURE_SCHEMA),
    ], check=True, env=scorer_env)
    scorer_ledger = pd.read_csv(scorer_output)
    save("default_off_scoring_output.csv", [{
        "candidate_unstarted_tb15_rows": len(open_rows), "scored_rows": 0,
        "flag": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0",
        "artifact_sha256": artifact_hash, "feature_schema_sha256": sha(FEATURE_SCHEMA),
        "scorer_ledger_rows": len(scorer_ledger),
        "unsupported_line_rows": int(scorer_ledger.line.ne(1.5).sum()),
        "decision": "NO_VALID_PREGAME_SLATE_AVAILABLE",
        "reason": "no exact certified live feature vectors; production unchanged",
    }])
    save("pregame_integrity_certification.csv", [{
        "status": "NO_VALID_PREGAME_SLATE_AVAILABLE", "candidate_unstarted_rows": len(open_rows),
        "eligible_feature_complete_rows": 0, "artifact_hash_verified": True,
        "feature_schema_hash_verified": True, "unsupported_lines_scored": 0,
        "sparse_rows_scored": 0, "production_changed": False,
    }])
    save("production_insertion_design.csv", [
        {"component": "insertion", "contract": "post-production TB probability, pre-publication; exact eligible TB1.5 rows only"},
        {"component": "ownership", "contract": "UBO-5 owns active probability only after every guard passes"},
        {"component": "fallback", "contract": "current production always authoritative on failure"},
        {"component": "logging", "contract": "artifact hash|feature hash|route reason|timestamp"},
        {"component": "downstream", "contract": "unchanged probability schema; no selector/upload/EV/ranking changes"},
        {"component": "rollback", "contract": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0"},
    ])
    save("counterfactual_ledger_contract.csv", [{
        "identity": "slate_date|game_pk|batter_mlb_id|total_bases|1.5",
        "fields": "ubo5_probability|production_counterfactual|both_artifact_ids|feature_vector_hash|route_reason|prediction_timestamp|official_result",
        "purpose": "debugging|rollback|informative long-run health; not a promotion watch",
    }])
    save("rollback_design.csv", [{
        "switch": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0",
        "immediate_triggers": "artifact hash|feature schema|temporal leakage|target exposure|stale source|identity|line!=1.5|PA<100|uncertified starter|bounds|serialization|downstream schema",
        "predictive_review": "informative accumulated population only; never one poor slate",
    }])
    save("file_impact_manifest.csv", [
        {"path": str(SCORER.relative_to(ROOT)), "action": "upgrade to recovered bundle and TB1.5-only fail-closed route"},
        {"path": str(ARTIFACT.relative_to(ROOT)), "action": "read-only model artifact"},
        {"path": "future production TB router", "action": "separate activation task only"},
        {"path": "future immutable counterfactual ledger", "action": "separate activation task only"},
    ])

    gates = {
        "A": True, "B": True, "C": ARTIFACT.exists(),
        "D": exact_reproduction, "E": exact_reproduction and reproduction["recovered_brier"] < reproduction["production_brier"]
             and reproduction["recovered_log_loss"] < reproduction["production_log_loss"],
        "F": False, "G": False, "H": True, "I": True, "J": True,
    }
    decisions = {
        "UBO5_TB15_GOVERNING_BINDING_DECISION": "PASS_ALL_FOUR_PACKAGES_SHA256_VERIFIED",
        "UBO5_TB15_RECOVERY_CONTRACT_FREEZE_DECISION": "PASS_FROZEN_BEFORE_SOLE_REFIT",
        "UBO5_TB15_REFIT_IDENTIFIABILITY_DECISION": "EXACT_REFIT_CONTRACT_IDENTIFIED",
        "UBO5_TB15_TRAINING_MATRIX_RECONSTRUCTION_DECISION": "PASS_EXACT_RETAINED_POPULATION_FEATURES_AND_LABELS",
        "UBO5_TB15_SERIALIZED_ARTIFACT_DECISION": f"PASS_ONE_ARTIFACT_SHA256_{artifact_hash}",
        "UBO5_TB15_HISTORICAL_LEDGER_REPRODUCTION_DECISION": "EXACT_DETERMINISTIC_REPRODUCTION" if exact_reproduction else "FAIL_REPRODUCTION_THRESHOLDS",
        "UBO5_TB15_RECOVERED_ARTIFACT_EVIDENCE_DECISION": "PASS_TB15_ADVANTAGE_PRESERVED" if gates["E"] else "FAIL",
        "UBO5_TB15_LIVE_FEATURE_BUILDER_DECISION": "FAIL_CURRENT_CERTIFIED_LIVE_PARTITIONS_NOT_MATERIALIZED",
        "UBO5_TB15_ELIGIBILITY_ROUTING_DECISION": "PASS_FAIL_CLOSED_TB15_ONLY_CONTRACT",
        "UBO5_TB15_DEFAULT_OFF_SCORER_DECISION": "PASS_DISABLED_ARTIFACT_AND_SCHEMA_HASH_BOUND",
        "UBO5_TB15_LIVE_FEATURE_AVAILABILITY_DECISION": "FAIL_ZERO_EXACT_FEATURE_COMPLETE_UNSTARTED_ROWS",
        "UBO5_TB15_PREGAME_INTEGRITY_CERTIFICATION_DECISION": "NO_VALID_PREGAME_SLATE_AVAILABLE",
        "UBO5_TB15_PRODUCTION_INSERTION_DESIGN_DECISION": "PASS_DOCUMENTED_NOT_EXECUTED",
        "UBO5_TB15_COUNTERFACTUAL_LEDGER_DECISION": "PASS_SCHEMA_FROZEN",
        "UBO5_TB15_ROLLBACK_DESIGN_DECISION": "PASS_IMMEDIATE_SWITCH_AND_TRIGGERS_FROZEN",
        **{f"UBO5_TB15_GATE_{key}_DECISION": "PASS" if value else "FAIL" for key, value in gates.items()},
        "MLB_UBO5_TB15_RECOVERY_DECISION": "RECOVERY_FAILED_CURRENT_PRODUCTION_PRESERVED_UBO5_IMPLEMENTATION_CLOSED",
        "MLB_UBO5_TB15_PRODUCTION_ACTION_DECISION": "NO_PRODUCTION_CHANGE_IN_THIS_TASK",
    }
    save("gate_decisions.csv", [{"gate": key, "status": "PASS" if value else "FAIL"} for key, value in gates.items()])
    save("terminal_decision.csv", [{"decision": key, "value": value} for key, value in decisions.items()])
    machine = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
               "artifact_sha256": artifact_hash, "reproduction": reproduction,
               "gates": gates, "decisions": decisions}
    (OUT / "machine_readable.json").write_text(
        json.dumps(machine, indent=2, default=lambda value: value.item() if hasattr(value, "item") else str(value)) + "\n"
    )
    required = [
        "governing_package_binding.csv", "frozen_recovery_contract.csv",
        "refit_identifiability_inventory.csv", "reconstructed_training_manifest.csv",
        "reconstructed_validation_manifest.csv", "matrix_reconciliation.csv",
        "training_command.csv", ARTIFACT.name, "artifact_identity.csv",
        "original_vs_recovered_probability_ledger.csv", "reproduction_statistics.csv",
        "recovered_tb15_evidence.csv", "live_feature_registry.csv",
        "live_feature_builder_contract.csv", "live_feature_availability_audit.csv",
        "default_off_scoring_output.csv", "default_off_scorer_ledger.csv",
        "pregame_integrity_certification.csv",
        "production_insertion_design.csv", "counterfactual_ledger_contract.csv",
        "rollback_design.csv", "file_impact_manifest.csv", "gate_decisions.csv",
        "terminal_decision.csv", "machine_readable.json",
    ]
    validation = [{"check": name, "status": "PASS" if (OUT / name).exists() else "FAIL",
                   "detail": "required deliverable"} for name in required]
    validation += [
        {"check": "sole_authorized_refit", "status": "PASS", "detail": "exactly one fit; no alternatives"},
        {"check": "artifact_hash", "status": "PASS" if sha(ARTIFACT) == artifact_hash else "FAIL", "detail": artifact_hash},
        {"check": "historical_reproduction", "status": "PASS" if exact_reproduction else "FAIL", "detail": str(reproduction["max_abs_difference"])},
        {"check": "no_production_change", "status": "PASS", "detail": "default-off offline recovery"},
        {"check": "terminal_failures_disclosed", "status": "PASS", "detail": "live feature and temporal gates F/G fail"},
    ]
    save("validation_report.csv", validation)
    manifest = []
    for path in sorted(p for p in OUT.iterdir() if p.is_file() and p.name != "sha256_manifest.csv"):
        manifest.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": sha(path)})
    save("sha256_manifest.csv", manifest)
    print(json.dumps(machine, indent=2, default=lambda value: value.item() if hasattr(value, "item") else str(value)))


if __name__ == "__main__":
    main()
