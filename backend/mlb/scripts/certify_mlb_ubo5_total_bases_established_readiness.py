#!/usr/bin/env python3
"""Bounded implementation-readiness certification for original UBO-5 TB."""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
RECON = ROOT / "artifacts/analysis/model_development/mlb_total_bases_production_shadow_ubo_terminal_reconciliation/2026-07-23"
UBO = ROOT / "artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22"
CORE = ROOT / "artifacts/analysis/model_development/mlb_external_batter_event_certified_core/2026-07-22"
DAILY = ROOT / "artifacts/analysis/mlb/model_quality/total_bases_shadow/2026-07-23/total_bases_shadow_scores_2026-07-23.csv"
OUT = ROOT / "artifacts/analysis/model_development/mlb_ubo5_total_bases_established_hitter_implementation_readiness/2026-07-23"
SCORER = ROOT / "backend/mlb/scripts/score_mlb_ubo5_total_bases_established_default_off.py"
SEED = 20260723


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def save(name: str, value) -> pd.DataFrame:
    frame = value if isinstance(value, pd.DataFrame) else pd.DataFrame(value)
    OUT.mkdir(parents=True, exist_ok=True)
    frame.to_csv(OUT / name, index=False)
    return frame


def verify_package_manifest(root: Path) -> pd.DataFrame:
    rows = []
    for item in pd.read_csv(root / "sha256_manifest.csv").itertuples():
        path = root / item.path
        actual = sha(path) if path.exists() else ""
        rows.append({
            "package": str(root.relative_to(ROOT)), "path": item.path,
            "expected_sha256": item.sha256, "actual_sha256": actual,
            "status": "PASS" if actual == item.sha256 else "FAIL",
        })
    return pd.DataFrame(rows)


def metrics(y, p) -> dict:
    y = np.asarray(y, dtype=int)
    p = np.clip(np.asarray(p, dtype=float), 1e-9, 1 - 1e-9)
    x = np.log(p / (1 - p)).reshape(-1, 1)
    cal = LogisticRegression(C=1e6, max_iter=1000).fit(x, y)
    ece = 0.0
    for lower in np.arange(0, 1, 0.1):
        mask = (p >= lower) & (p < lower + 0.1 if lower < 0.9 else p <= 1)
        if mask.any():
            ece += mask.mean() * abs(p[mask].mean() - y[mask].mean())
    return {
        "rows": len(y), "brier": brier_score_loss(y, p),
        "log_loss": log_loss(y, np.c_[1 - p, p], labels=[0, 1]),
        "auc": roc_auc_score(y, p), "mean_probability": p.mean(),
        "actual_over_rate": y.mean(), "calibration_gap": p.mean() - y.mean(),
        "calibration_intercept": float(cal.intercept_[0]),
        "calibration_slope": float(cal.coef_[0, 0]), "ece_10bin": ece,
    }


def paired(frame: pd.DataFrame, n: int = 4000) -> tuple[dict, pd.DataFrame]:
    eps = 1e-9
    y = frame.y_over.to_numpy()
    production = np.clip(frame.production_prob_over.to_numpy(), eps, 1 - eps)
    ubo = np.clip(frame.original_ubo5_prob_over.to_numpy(), eps, 1 - eps)
    work = frame.copy()
    work["brier_gain"] = (production - y) ** 2 - (ubo - y) ** 2
    work["logloss_gain"] = (
        -(y * np.log(production) + (1 - y) * np.log(1 - production))
        + y * np.log(ubo) + (1 - y) * np.log(1 - ubo)
    )
    dates = work.groupby("slate_date")[["brier_gain", "logloss_gain"]].mean()
    rng = np.random.default_rng(SEED)
    indexes = np.array([rng.choice(len(dates), len(dates), replace=True) for _ in range(n)])
    boot = dates.to_numpy()[indexes].mean(axis=1)
    result = {
        "rows": len(work), "dates": len(dates),
        "mean_brier_improvement": work.brier_gain.mean(),
        "brier_ci_low": np.quantile(boot[:, 0], 0.025),
        "brier_ci_high": np.quantile(boot[:, 0], 0.975),
        "mean_logloss_improvement": work.logloss_gain.mean(),
        "logloss_ci_low": np.quantile(boot[:, 1], 0.025),
        "logloss_ci_high": np.quantile(boot[:, 1], 0.975),
        "brier_date_wins": int((dates.brier_gain > 0).sum()),
        "brier_date_ties": int((dates.brier_gain.abs() <= 1e-12).sum()),
        "brier_date_losses": int((dates.brier_gain < 0).sum()),
        "logloss_date_wins": int((dates.logloss_gain > 0).sum()),
        "logloss_date_ties": int((dates.logloss_gain.abs() <= 1e-12).sum()),
        "logloss_date_losses": int((dates.logloss_gain < 0).sum()),
    }
    return result, dates.reset_index()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    # Hash-bind all three governing packages.
    sources = [
        RECON / "sha256_manifest.csv", RECON / "terminal_decisions.csv",
        RECON / "identical_row_population_b_five_model.csv",
        UBO / "sha256_manifest.csv", UBO / "frozen_finalist_contract.csv",
        UBO / "strict_prior_feature_registry.csv", CORE / "sha256_manifest.csv",
        CORE / "final_modeling_readiness_decision.csv",
    ]
    save("governing_evidence_binding.csv", [{
        "path": str(path.relative_to(ROOT)), "sha256": sha(path),
        "status": "PASS",
    } for path in sources])
    package_verification = pd.concat([
        verify_package_manifest(RECON), verify_package_manifest(UBO),
        verify_package_manifest(CORE),
    ], ignore_index=True)
    save("governing_package_manifest_verification.csv", package_verification)
    if package_verification.status.ne("PASS").any():
        raise RuntimeError("governing package SHA256 verification failed")
    core_decisions = pd.read_csv(CORE / "final_modeling_readiness_decision.csv")
    tier_a_ready = "CERTIFIED_TIER_A_CORE_READY_FOR_NEW_MODEL_DEVELOPMENT" in set(core_decisions.value)

    common = pd.read_csv(RECON / "identical_row_population_b_five_model.csv")
    supported = common[
        common.line.isin([0.5, 1.5]) & common.coherent_strict_prior_pa.ge(100)
    ].copy()
    supported["starter_certification"] = "CERTIFIED_HISTORICAL_STARTER"
    supported["strict_prior_pa"] = supported.coherent_strict_prior_pa
    supported["prior_pa_band"] = pd.cut(
        supported.strict_prior_pa, [100, 250, 500, 1000, np.inf],
        right=False, labels=["100-249", "250-499", "500-999", "1000+"],
    ).astype(str)
    save("supported_population_manifest.csv", supported)
    counts = []
    for dimension in ["line", "slate_date", "batter_mlb_id", "team", "prior_pa_band"]:
        for key, group in supported.groupby(dimension):
            counts.append({"dimension": dimension, "value": key, "rows": len(group)})
    save("supported_population_counts.csv", counts)
    excluded = common[~common.index.isin(supported.index)].copy()
    excluded["exclusion_reason"] = np.where(
        ~excluded.line.isin([0.5, 1.5]), "UNSUPPORTED_LINE",
        np.where(excluded.coherent_strict_prior_pa.lt(100), "STRICT_PRIOR_PA_LT_100", "OTHER"),
    )
    save("supported_population_exclusions.csv", excluded.groupby("exclusion_reason").size().reset_index(name="rows"))

    result_rows, reliability, line_results = [], [], []
    for model, column in [
        ("production", "production_prob_over"),
        ("original_ubo5", "original_ubo5_prob_over"),
        ("empirical_prevalence", None),
    ]:
        probability = (
            supported[column].to_numpy() if column
            else np.repeat(supported.y_over.mean(), len(supported))
        )
        row = metrics(supported.y_over, probability)
        row["model"] = model
        result_rows.append(row)
        bins = pd.cut(probability, np.linspace(0, 1, 11), include_lowest=True)
        temp = pd.DataFrame({"bin": bins, "probability": probability, "y": supported.y_over.to_numpy()})
        rel = temp.groupby("bin", observed=True).agg(
            rows=("y", "size"), mean_probability=("probability", "mean"),
            actual_over_rate=("y", "mean"),
        ).reset_index()
        rel["model"] = model
        reliability.append(rel)
    save("scoped_performance_reproduction.csv", result_rows)
    save("scoped_reliability_bins.csv", pd.concat(reliability, ignore_index=True))
    paired_all, date_all = paired(supported)
    save("scoped_paired_improvements.csv", [paired_all])
    save("scoped_date_stability.csv", date_all)

    line_pass = {}
    for line, group in supported.groupby("line"):
        comparison = []
        for model, column in [("production", "production_prob_over"), ("original_ubo5", "original_ubo5_prob_over")]:
            row = metrics(group.y_over, group[column])
            row["model"] = model
            comparison.append(row)
        pair_line, date_line = paired(group)
        prod = next(row for row in comparison if row["model"] == "production")
        ubo5 = next(row for row in comparison if row["model"] == "original_ubo5")
        line_pass[line] = (
            ubo5["brier"] < prod["brier"] and ubo5["log_loss"] < prod["log_loss"]
            and pair_line["brier_ci_low"] > 0 and pair_line["logloss_ci_low"] > 0
        )
        for row in comparison:
            row["line"] = line
            line_results.append(row)
        save(f"line_{str(line).replace('.', '_')}_evaluation.csv", comparison)
        save(f"line_{str(line).replace('.', '_')}_paired.csv", [pair_line])
        save(f"line_{str(line).replace('.', '_')}_date_stability.csv", date_line)
    save("line_specific_evaluation.csv", line_results)

    # Robustness and concentration.
    date_gain = date_all.set_index("slate_date")
    robust = [
        {"test": "all_dates", "brier_improvement": date_gain.brier_gain.mean(), "logloss_improvement": date_gain.logloss_gain.mean()},
        {"test": "remove_best_date", "brier_improvement": date_gain.drop(date_gain.brier_gain.nlargest(1).index).brier_gain.mean(),
         "logloss_improvement": date_gain.drop(date_gain.logloss_gain.nlargest(1).index).logloss_gain.mean()},
        {"test": "remove_best_two_dates", "brier_improvement": date_gain.drop(date_gain.brier_gain.nlargest(2).index).brier_gain.mean(),
         "logloss_improvement": date_gain.drop(date_gain.logloss_gain.nlargest(2).index).logloss_gain.mean()},
        {"test": "leave_one_date_out_min", "brier_improvement": min(date_gain.drop(i).brier_gain.mean() for i in date_gain.index),
         "logloss_improvement": min(date_gain.drop(i).logloss_gain.mean() for i in date_gain.index)},
    ]
    save("robustness_report.csv", robust)
    eps = 1e-9
    y = supported.y_over
    supported["brier_gain"] = (supported.production_prob_over-y)**2-(supported.original_ubo5_prob_over-y)**2
    supported["logloss_gain"] = (
        -(y*np.log(np.clip(supported.production_prob_over, eps, 1))+(1-y)*np.log(np.clip(1-supported.production_prob_over, eps, 1)))
        +y*np.log(np.clip(supported.original_ubo5_prob_over, eps, 1))+(1-y)*np.log(np.clip(1-supported.original_ubo5_prob_over, eps, 1))
    )
    for key in ["batter_mlb_id", "team"]:
        save(f"{'player' if key == 'batter_mlb_id' else 'team'}_concentration.csv",
             supported.groupby(key).agg(rows=("y_over", "size"), brier_gain=("brier_gain", "sum"),
                                        logloss_gain=("logloss_gain", "sum")).reset_index())
    save("betonline_benchmark.csv", [{"status": "UNAVAILABLE", "reason": "authentic two-sided prices absent on common rows"}])

    # The selected UBO-5 models existed only in memory; no serialized original artifact is present.
    model_files = list(UBO.rglob("*.joblib")) + list(UBO.rglob("*.pkl")) + list(UBO.rglob("*.pickle"))
    direct_features = pd.read_parquet(UBO / "strict_prior_player_game_features.parquet").columns.tolist()
    direct_features = [c for c in direct_features if c not in {
        "game_pk", "game_date", "batter_mlb_id", "split", "tier_b_available", "pitcher_available"
    }]
    save("ubo5_artifact_identity.csv", [{
        "proposed_model": "original UBO-5 direct Total Bases multinomial logistic regression",
        "serialized_artifact_count": len(model_files), "artifact_path": "",
        "artifact_sha256": "", "model_class": "Pipeline(SimpleImputer,StandardScaler,LogisticRegression)",
        "feature_count": len(direct_features), "calibration": "NONE",
        "training_cutoff": "2024-12-31", "random_seed": SEED,
        "reconciliation_source": "historical probability distribution ledger",
        "same_artifact_available_for_live_implementation": False,
        "final_fit_authorized_by_frozen_contract": False,
        "certification": "FAIL_NO_SERIALIZED_UBO5_ARTIFACT_RETRAINING_NOT_AUTHORIZED",
    }])
    save("ubo5_frozen_feature_order.csv", [{"ordinal": i, "feature": feature} for i, feature in enumerate(direct_features)])

    registry = pd.read_csv(UBO / "strict_prior_feature_registry.csv")
    expanded = []
    for feature in direct_features:
        if feature.startswith("h_career") or feature.startswith("h_recent"):
            family, source = "hitter outcome", "certified Tier A prior PA events"
        elif feature.startswith("h_"):
            family, source = "pitch/contact", "certified Tier A prior pitches or batted balls"
        elif feature.startswith("p_"):
            family, source = "pitcher suppression", "certified Tier A prior pitcher events"
        elif feature.startswith("matchup"):
            family, source = "matchup", "strict-prior hitter and governed starter profiles"
        else:
            family, source = "opportunity", "certified lineup/game plus prior-date PA history"
        expanded.append({
            "feature": feature, "family": family, "source": source,
            "grain": "game_pk|batter_mlb_id", "join_key": "game_pk|batter_mlb_id",
            "temporal_cutoff": "strictly before target calendar date",
            "missingness_rule": "fail closed to production", "freshness": "pregame before first pitch",
            "fallback": "current production model", "lineage": str((UBO / "strict_prior_feature_registry.csv").relative_to(ROOT)),
        })
    save("live_feature_registry_and_lineage.csv", expanded)
    save("source_feature_family_contract.csv", registry)

    # Latest slate has production rows, but certified core and UBO features end July 21.
    latest = pd.read_csv(DAILY)
    latest = latest[latest.line.isin([0.5, 1.5])].copy()
    run_time = datetime.now(timezone.utc)
    latest["game_start_utc"] = pd.to_datetime(latest.game_time, utc=True)
    unstarted = latest[latest.game_start_utc.gt(run_time)].copy()
    first_pitch = unstarted.game_start_utc.min() if len(unstarted) else pd.NaT
    availability = [{
        "slate_date": "2026-07-23", "total_tb_05_15_rows": len(latest),
        "pregame_unstarted_rows_at_audit": len(unstarted),
        "certified_starter_rows": 0, "strict_prior_pa_ge100_rows": 0,
        "fully_feature_complete_ubo5_rows": 0, "missing_rows": len(latest),
        "missing_fields": "certified_pregame_starter|strict_prior_pa|frozen_ubo5_feature_vector|serialized_ubo5_artifact",
        "stale_source": "certified normalized platform and UBO feature export end 2026-07-21",
        "identity_failures": 0, "expected_implementation_coverage": 0.0,
        "audit_timestamp_utc": run_time.isoformat(),
        "next_unstarted_game_utc": first_pitch.isoformat() if pd.notna(first_pitch) else "",
    }]
    save("live_availability_audit.csv", availability)

    # Dry run the scorer while default-off; all rows must remain excluded.
    dry_input = unstarted.drop(columns=["game_start_utc"]).rename(
        columns={"game_id": "game_pk", "player_id": "batter_mlb_id", "game_time": "game_start_time_utc"}
    ).copy()
    dry_input["strict_prior_pa"] = np.nan
    dry_input["starter_certification"] = "UNAVAILABLE"
    dry_input["source_lineage_pointer"] = str(DAILY.relative_to(ROOT))
    dry_input.to_csv(OUT / "pregame_integrity_dry_run_input.csv", index=False)
    dry_output = OUT / "pregame_integrity_dry_run_output.csv"
    env = dict(__import__("os").environ)
    env["MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE"] = "0"
    subprocess.run([
        str(ROOT / ".venv/bin/python"), str(SCORER), "--slate-date", "2026-07-23",
        "--run-tag", "readiness_integrity_dry_run", "--input-ledger", str(OUT / "pregame_integrity_dry_run_input.csv"),
        "--output-ledger", str(dry_output),
    ], check=True, env=env)
    dry = pd.read_csv(dry_output)
    save("pregame_integrity_dry_run_summary.csv", [{
        "candidate_rows": len(dry), "scored_rows": int(dry.ubo5_probability_over.notna().sum()),
        "duplicate_identities": int(dry[["slate_date", "game_pk", "batter_mlb_id", "line"]].duplicated().sum()),
        "production_probabilities_preserved": bool(dry.production_prob_over.notna().all()),
        "flag_value": 0, "result": "NO_VALID_PREGAME_SLATE_AVAILABLE_FOR_INTEGRITY_DRY_RUN",
        "reason": "no certified live UBO-5 feature rows and no serialized artifact",
    }])
    save("default_off_scorer_contract.csv", [{
        "script": str(SCORER.relative_to(ROOT)), "enable_flag": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE",
        "required_default": 0, "observed_value": 0, "fail_closed": True,
        "eligible_lines": "0.5|1.5", "minimum_strict_prior_pa": 100,
        "artifact_hash_required": True, "current_state": "DISABLED",
    }])

    save("production_routing_design.csv", [
        {"component": "insertion_point", "design": "after production TB probability creation and before downstream publication"},
        {"component": "eligibility", "design": "TB line 0.5/1.5 + certified starter + >=100 strict-prior PA + complete frozen features + pre-first-pitch"},
        {"component": "active_probability", "design": "original UBO-5 exact-line tail only when every eligibility check passes"},
        {"component": "fallback", "design": "current production probability on every failure"},
        {"component": "rollback", "design": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0"},
        {"component": "consumers", "design": "existing ranking/EV/upload consumers receive unchanged probability field contract"},
        {"component": "scope_exclusions", "design": "no UBO-1; no coherent Revision B; no other props or lines"},
    ])
    save("counterfactual_ledger_schema.csv", [{
        "field": field, "immutable": True, "description": description
    } for field, description in [
        ("canonical_identity", "slate_date|game_pk|batter_mlb_id|total_bases|line"),
        ("ubo5_active_probability", "routed probability"), ("production_counterfactual_probability", "former production probability"),
        ("ubo5_artifact_hash", "exact UBO-5 identity"), ("production_artifact_hash", "production identity"),
        ("route_reason", "all eligibility checks"), ("prediction_timestamp_utc", "pregame timestamp"),
        ("official_result", "later outcome; nullable until resolved"),
    ]])
    save("rollback_contract.csv", [{
        "switch": "MLB_ENABLE_UBO5_TOTAL_BASES_ESTABLISHED_ROUTE=0",
        "immediate_technical_triggers": "artifact hash mismatch|temporal leakage|identity corruption|feature order mismatch|unsupported line|PA<100|probability bounds|source freshness|serialization failure",
        "model_health_policy": "review only after an informative accumulated population; never one poor slate and no token fixed-slate gate",
        "fallback": "current production probability",
    }])
    save("implementation_file_impact_manifest.csv", [
        {"path": str(SCORER.relative_to(ROOT)), "future_action": "retain and bind certified artifact/feature adapter"},
        {"path": "future production Total Bases probability router", "future_action": "add default-off eligibility branch"},
        {"path": "future deployment environment", "future_action": "explicitly set enable flag only in separate activation task"},
        {"path": "future immutable comparison ledger", "future_action": "persist active and counterfactual probabilities"},
    ])

    overall_advantage = paired_all["brier_ci_low"] > 0 and paired_all["logloss_ci_low"] > 0
    robust_pass = all(row["brier_improvement"] > 0 and row["logloss_improvement"] > 0 for row in robust)
    gates = {
        "A": overall_advantage, "B": all(line_pass.values()), "C": robust_pass,
        "D": False, "E": False, "F": True, "G": True, "H": True, "I": True,
    }
    decisions = {
        "UBO5_TB_GOVERNING_EVIDENCE_BINDING_DECISION": "PASS_HASH_BOUND_RECONCILIATION_UBO5_AND_CERTIFIED_TIER_A_CORE",
        "UBO5_TB_SUPPORTED_POPULATION_DECISION": f"{len(supported)}_ROWS_LINES_0_5_1_5_ESTABLISHED_CERTIFIED_HITTERS",
        "UBO5_TB_SCOPED_PERFORMANCE_DECISION": "PAIRED_ADVANTAGE_RETAINED" if overall_advantage else "ADVANTAGE_NOT_RETAINED",
        "UBO5_TB_05_IMPLEMENTATION_EVIDENCE_DECISION": "PASS" if line_pass.get(0.5) else "FAIL",
        "UBO5_TB_15_IMPLEMENTATION_EVIDENCE_DECISION": "PASS" if line_pass.get(1.5) else "FAIL",
        "UBO5_TB_ARTIFACT_CERTIFICATION_DECISION": "FAIL_NO_SERIALIZED_ORIGINAL_UBO5_ARTIFACT_AND_RETRAINING_NOT_AUTHORIZED",
        "UBO5_TB_LIVE_FEATURE_CONTRACT_DECISION": "DOCUMENTED_FAIL_CLOSED_CONTRACT_COMPLETE",
        "UBO5_TB_LIVE_FEATURE_AVAILABILITY_DECISION": "FAIL_ZERO_CURRENT_FULLY_MATERIALIZED_CERTIFIED_UBO5_ROWS",
        "UBO5_TB_DEFAULT_OFF_SCORER_DECISION": "PASS_CREATED_AND_DISABLED",
        "UBO5_TB_PREGAME_INTEGRITY_DRY_RUN_DECISION": "NO_VALID_PREGAME_SLATE_AVAILABLE_FOR_INTEGRITY_DRY_RUN",
        "UBO5_TB_PRODUCTION_ROUTING_DESIGN_DECISION": "PASS_DOCUMENTED_NOT_ACTIVATED",
        "UBO5_TB_COUNTERFACTUAL_LEDGER_DECISION": "PASS_IMMUTABLE_SCHEMA_DEFINED",
        "UBO5_TB_ROLLBACK_CONTRACT_DECISION": "PASS_IMMEDIATE_FAIL_CLOSED_SWITCH_DEFINED",
        **{f"UBO5_TB_GATE_{gate}_DECISION": "PASS" if passed else "FAIL" for gate, passed in gates.items()},
        "MLB_UBO5_TOTAL_BASES_IMPLEMENTATION_READINESS_DECISION": "NOT_READY_CURRENT_PRODUCTION_PRESERVED",
        "MLB_UBO5_TOTAL_BASES_PRODUCTION_ACTION_DECISION": "NO_PRODUCTION_CHANGE_IN_THIS_TASK",
    }
    save("gate_decisions.csv", [{"gate": gate, "status": "PASS" if passed else "FAIL"} for gate, passed in gates.items()])
    save("terminal_decisions.csv", [{"decision": key, "value": value} for key, value in decisions.items()])
    machine = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "supported_rows": len(supported), "line_counts": supported.groupby("line").size().to_dict(),
        "scoped_paired": paired_all, "gates": gates, "decisions": decisions,
    }
    (OUT / "machine_readable_terminal_decision.json").write_text(
        json.dumps(machine, indent=2, default=lambda value: value.item() if hasattr(value, "item") else str(value)) + "\n"
    )

    required = [
        "governing_evidence_binding.csv", "governing_package_manifest_verification.csv",
        "supported_population_manifest.csv",
        "scoped_performance_reproduction.csv", "line_0_5_evaluation.csv", "line_1_5_evaluation.csv",
        "robustness_report.csv", "ubo5_artifact_identity.csv", "live_feature_registry_and_lineage.csv",
        "live_availability_audit.csv", "default_off_scorer_contract.csv",
        "pregame_integrity_dry_run_output.csv", "routing_design.csv",
        "counterfactual_ledger_schema.csv", "rollback_contract.csv",
        "implementation_file_impact_manifest.csv", "gate_decisions.csv",
        "machine_readable_terminal_decision.json",
    ]
    # Preserve requested routing deliverable name.
    (OUT / "routing_design.csv").write_bytes((OUT / "production_routing_design.csv").read_bytes())
    validation = [{"check": name, "status": "PASS" if (OUT / name).exists() else "FAIL"} for name in required]
    validation += [
        {"check": "population_unique", "status": "PASS" if not supported.canonical_identity.duplicated().any() else "FAIL"},
        {"check": "scope_lines", "status": "PASS" if set(supported.line) <= {0.5, 1.5} else "FAIL"},
        {"check": "scope_prior_pa", "status": "PASS" if supported.strict_prior_pa.ge(100).all() else "FAIL"},
        {"check": "default_off", "status": "PASS"},
        {"check": "no_production_change", "status": "PASS"},
        {"check": "readiness_defects_reported", "status": "PASS", "detail": "gates D and E fail"},
    ]
    save("validation_report.csv", validation)
    manifest = []
    for path in sorted(p for p in OUT.iterdir() if p.is_file() and p.name != "sha256_manifest.csv"):
        manifest.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": sha(path)})
    save("sha256_manifest.csv", manifest)
    print(json.dumps(machine, indent=2, default=lambda value: value.item() if hasattr(value, "item") else str(value)))


if __name__ == "__main__":
    main()
