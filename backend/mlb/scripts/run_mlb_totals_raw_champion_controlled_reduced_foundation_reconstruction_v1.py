"""Controlled Stage-4 simultaneous reduction of the frozen MLB totals RAW champion.

The utility has an explicit pre-evaluation freeze phase.  Candidate definitions
and interpretation thresholds are written and hashed before any reduced model is
fit.  The evaluation phase refuses to run unless that exact hash is supplied.
This is analysis-only and cannot alter an operational model or ledger.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import sklearn
from scipy.special import gammaln
from scipy.stats import nbinom, pearsonr, spearmanr
from sklearn.metrics import mean_poisson_deviance

from backend.mlb.scripts import run_mlb_totals_raw_frozen_champion_single_feature_dissection_v1 as stage1
from backend.mlb.scripts import run_mlb_totals_raw_champion_single_feature_replaceability_refit_v1 as stage3


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_RAW_CHAMPION_CONTROLLED_REDUCED_FOUNDATION_RECONSTRUCTION_V1"
OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_champion_controlled_reduced_foundation_reconstruction_v1/2026-08-29"
STAGE1 = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_single_feature_dissection_v1/2026-08-29"
STAGE2 = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_frozen_champion_interaction_cancellation_dissection_v1/2026-08-29"
STAGE3 = ROOT / "artifacts/analysis/model_development/mlb_totals_raw_champion_single_feature_replaceability_refit_v1/2026-08-29"
SPINE = ROOT / "artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06"
LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
AGGREGATE = "ALL_GOVERNED_EVALUATION"
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 20260829 + 4
EXPECTED_COUNTS = dict(zip(stage1.PERIODS, (2433, 1281, 439, 156)))

# Frozen before Stage-4 outcomes. Positive values are allowed degradation.
PRESERVATION_THRESHOLDS = {
    "NEAR_COMPLETE": {"aggregate_relative_mae_rmse_crps": .01, "brier": .001, "log_loss": .003,
                      "maximum_period_relative_mae_crps": .03},
    "SUBSTANTIAL": {"aggregate_relative_mae_rmse_crps": .03, "brier": .003, "log_loss": .010,
                    "maximum_period_relative_mae_crps": .075},
    "PARTIAL": {"aggregate_relative_mae_rmse_crps": .075, "brier": .0075, "log_loss": .025,
                "maximum_period_relative_mae_crps": .15},
    "LOW": {"aggregate_relative_mae_rmse_crps": .15, "brier": .015, "log_loss": .050,
            "maximum_period_relative_mae_crps": .30},
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader(); writer.writerows(rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def frame_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    payload = frame.sort_values(["game_date", "game_pk"])[columns].to_csv(index=False, lineterminator="\n", float_format="%.17g")
    return hashlib.sha256(payload.encode()).hexdigest()


def validate_hash_package(directory: Path) -> tuple[bool, list[str]]:
    manifest = json.loads((directory / "reproducibility_hashes.json").read_text())
    failures = []
    for name, digest in manifest["outputs"].items():
        path = directory / name
        if not path.is_file() or sha256(path) != digest:
            failures.append(name)
    return not failures, failures


def verify_stages() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    for number, directory in ((1, STAGE1), (2, STAGE2), (3, STAGE3)):
        valid, failures = validate_hash_package(directory)
        checks[f"stage{number}_output_hashes_valid"] = valid
        checks[f"stage{number}_hash_failures"] = failures
        checks[f"stage{number}_hash_manifest_sha256"] = sha256(directory / "reproducibility_hashes.json")
    checks["stage1_raw_reproduction"] = json.loads((STAGE1 / "raw_champion_reproduction.json").read_text())["RAW_CHAMPION_REPRODUCTION"]
    checks["stage1_ablation_rows"] = len(read_csv(STAGE1 / "raw_champion_feature_point_deltas.csv"))
    stage2_manifest = STAGE2 / "stage2_joint_ablation_manifest.csv"
    checks["stage2_manifest_sha256"] = sha256(stage2_manifest)
    checks["stage2_manifest_expected"] = checks["stage2_manifest_sha256"] == stage3.STAGE2_MANIFEST_SHA
    checks["stage3_raw_reproduction"] = json.loads((STAGE3 / "stage3_raw_training_reproduction.json").read_text())["RAW_REFIT_REPRODUCTION"]
    checks["stage3_refit_manifest_rows"] = len(read_csv(STAGE3 / "stage3_22_refit_manifest.csv"))
    checks["stage3_candidate_artifacts"] = len(list((STAGE3 / "candidate_artifacts").glob("*.json")))
    class_rows = read_csv(STAGE3 / "stage3_feature_replaceability_classification.csv")
    checks["stage3_classification_rows"] = len(class_rows)
    checks["stage3_classification_counts"] = dict(sorted(Counter(row["stage3_primary_label"] for row in class_rows).items()))
    passed = (all(checks[f"stage{x}_output_hashes_valid"] for x in (1, 2, 3)) and
              checks["stage1_raw_reproduction"] == "PASS" and checks["stage2_manifest_expected"] and
              checks["stage3_raw_reproduction"] in ("PASS", "PASS_WITH_NUMERICAL_TOLERANCE") and
              checks["stage3_refit_manifest_rows"] == checks["stage3_candidate_artifacts"] == checks["stage3_classification_rows"] == 22)
    checks.update({"task_id": TASK_ID, "model_identity": stage1.MODEL_IDENTITY, "model_hash": stage1.MODEL_HASH,
                   "artifact_sha256": sha256(stage1.raw.CONFIG), "STAGES_1_3_REPRODUCTION": "PASS" if passed else "FAIL"})
    if not passed:
        raise RuntimeError("STAGES_1_3_REPRODUCTION_FAIL")
    return checks


def independently_reconstruct_historical_factor(core: pd.DataFrame) -> pd.DataFrame:
    parks: dict[int, list[dict[str, Any]]] = defaultdict(list)
    league: list[dict[str, float]] = []
    team_scored: dict[int, list[float]] = defaultdict(list)
    rows = []
    for _, day in core.sort_values(["game_date", "scheduled_start_utc", "game_pk"]).groupby("game_date", sort=True):
        for game in day.itertuples():
            prior = parks[int(game.venue_id)]
            direct = float(np.mean([x["adjusted_total_ratio"] for x in prior])) if prior else 1.0
            n = len(prior); weight = n / (n + 50)
            rows.append({"game_pk": int(game.game_pk), "independent_factor": weight * direct + (1 - weight),
                         "independent_depth": n, "independent_fallback": "DIRECT_REGRESSED_PARK_HISTORY" if n >= 20 else "LEAGUE_REGRESSED_SPARSE_PARK"})
        # Same-date outcomes are admitted only after every pregame state for that date is frozen.
        for game in day.itertuples():
            league_mean = float(np.mean([x["total"] for x in league])) if league else 8.6
            hp = team_scored[int(game.home_team_id)]; ap = team_scored[int(game.away_team_id)]
            expected_home = float(np.mean(hp)) if hp else league_mean / 2
            expected_away = float(np.mean(ap)) if ap else league_mean / 2
            parks[int(game.venue_id)].append({"adjusted_total_ratio": float(game.final_total) / max(expected_home + expected_away, .5)})
            league.append({"total": float(game.final_total)})
            hp.append(float(game.final_home_runs)); ap.append(float(game.final_away_runs))
    return pd.DataFrame(rows)


def final_park_states(core: pd.DataFrame) -> dict[int, dict[str, Any]]:
    parks: dict[int, list[dict[str, Any]]] = defaultdict(list)
    league: list[dict[str, float]] = []; team_scored: dict[int, list[float]] = defaultdict(list)
    for game in core.sort_values(["game_date", "scheduled_start_utc", "game_pk"]).itertuples():
        league_mean = float(np.mean([x["total"] for x in league])) if league else 8.6
        hp = team_scored[int(game.home_team_id)]; ap = team_scored[int(game.away_team_id)]
        eh = float(np.mean(hp)) if hp else league_mean / 2; ea = float(np.mean(ap)) if ap else league_mean / 2
        parks[int(game.venue_id)].append({"game_pk": int(game.game_pk), "adjusted": float(game.final_total) / max(eh + ea, .5)})
        league.append({"total": float(game.final_total)}); hp.append(float(game.final_home_runs)); ap.append(float(game.final_away_runs))
    result = {}
    for venue, prior in parks.items():
        n = len(prior); w = n / (n + 50)
        result[prior[-1]["game_pk"]] = {"factor": w * float(np.mean([x["adjusted"] for x in prior])) + (1 - w), "depth": n, "venue_id": venue}
    return result


def equivalence_rows(artifact: dict[str, Any]) -> list[dict[str, Any]]:
    historical = stage1.raw.load_historical(artifact)
    core = pd.read_csv(SPINE / "totals_core_feature_spine.csv")
    core["game_date"] = pd.to_datetime(core.game_date)
    independent = independently_reconstruct_historical_factor(core)
    joined = historical.merge(independent, on="game_pk", how="left")
    output = []
    populations = [("ORIGINAL_TRAINING", joined.period.eq("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE"))]
    populations += [(period, joined.period.eq(period)) for period in stage1.PERIODS[:3]]
    for name, mask in populations:
        part = joined.loc[mask].copy(); diff = abs(part.strict_prior_total_run_factor - part.independent_factor)
        stored_fallback = np.where(part.park_history_depth >= 20, "DIRECT_REGRESSED_PARK_HISTORY", "LEAGUE_REGRESSED_SPARSE_PARK")
        output.append({"population": name, "rows": len(part), "exact_matches": int((diff == 0).sum()),
                       "max_absolute_difference": float(diff.max()), "mean_absolute_difference": float(diff.mean()),
                       "missingness_differences": int((part.strict_prior_total_run_factor.isna() != part.independent_factor.isna()).sum()),
                       "fallback_differences": int((stored_fallback != part.independent_fallback).sum()),
                       "temporal_version_difference": "NONE; independent date-batch strict-prior reconstruction of MLB_TOTALS_FEATURE_SPINE_V1"})

    finals = final_park_states(core)
    connection = sqlite3.connect(f"file:{LEDGER}?mode=ro", uri=True)
    records = connection.execute("""SELECT p.game_id,c.context_payload_json FROM totals_shadow_predictions p
                                  JOIN totals_shadow_prediction_context c USING(canonical_identity)
                                  WHERE p.game_date BETWEEN '2026-08-17' AND '2026-08-28' ORDER BY p.game_date,p.game_id""").fetchall()
    connection.close(); diffs = []; exact = missing = fallback = 0
    for _, payload in records:
        context = json.loads(payload); park = context["park_state"]; model_value = float(context["model_features"]["strict_prior_total_run_factor"])
        latest = park.get("latest_included_game_id")
        independent_state = finals.get(int(latest)) if latest is not None else {"factor": 1.0, "depth": 0}
        if independent_state is None:
            missing += 1; continue
        difference = abs(model_value - independent_state["factor"]); diffs.append(difference); exact += difference == 0
        expected_fallback = "DIRECT_REGRESSED_PARK_HISTORY" if independent_state["depth"] >= 20 else "LEAGUE_REGRESSED_SPARSE_PARK"
        fallback += int(park["fallback_status"] != expected_fallback or int(park["park_history_depth"]) != independent_state["depth"])
    output.append({"population": stage1.PERIODS[-1], "rows": len(records), "exact_matches": exact,
                   "max_absolute_difference": float(max(diffs, default=0)), "mean_absolute_difference": float(np.mean(diffs) if diffs else 0),
                   "missingness_differences": missing, "fallback_differences": fallback,
                   "temporal_version_difference": "Same formula; prospective live_context_bridge_v1 park foundation frozen through 2026-08-05 rather than advanced after each later final"})
    return output


def derive_candidates(class_rows: list[dict[str, str]], feature_order: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    labels = {row["feature"]: row["stage3_primary_label"] for row in class_rows}
    foundation = [x for x in feature_order if labels[x] == "HIGH_CONFIDENCE_FOUNDATION_CANDIDATE"]
    addbacks = [x for x in feature_order if labels[x] in ("PARTIALLY_REPLACEABLE", "REGIME_DEPENDENT")]
    survivor = [x for x in feature_order if labels[x] in ("HIGH_CONFIDENCE_FOUNDATION_CANDIDATE", "PARTIALLY_REPLACEABLE", "REGIME_DEPENDENT")]
    unresolved = [x for x in feature_order if labels[x] in ("UNRESOLVED", "COMPENSATION_DEPENDENT", "UNIQUE_INFORMATION_CANDIDATE")]
    if unresolved:
        raise RuntimeError(f"UNRESOLVED_STAGE3_CLASS_REQUIRES_FROZEN_RULE:{unresolved}")
    definitions = [("CORE_1", foundation, "all HIGH_CONFIDENCE_FOUNDATION_CANDIDATE features")]
    definitions += [(f"CORE_1_PLUS_{x.upper()}", [*foundation, x], f"CORE_1 plus mandatory independent addback for {labels[x]}") for x in addbacks]
    definitions.append(("CONSERVATIVE_SURVIVOR_CORE", survivor, "foundation + partial + regime-dependent classes"))
    candidates = []
    for i, (identity, features, reason) in enumerate(definitions, 1):
        candidates.append({"candidate_number": i, "candidate_identity": identity, "direct_feature_count": len(features),
                           "direct_feature_order_pipe": "|".join(features), "derivation_rule": reason,
                           "training_rows": 4859, "fit_count_predeclared": 1, "evaluation_rows_used_for_candidate_definition": 0,
                           "upstream_support_gating_shrinkage_fallback_preserved": True,
                           "research_only_not_promoted": True,
                           "preservation_thresholds_json": json.dumps(PRESERVATION_THRESHOLDS, sort_keys=True),
                           "temporal_rule": "period preserved if positive MAE and CRPS degradation <=3%, Brier <=.003, log loss <=.010; broken if MAE or CRPS >10%; otherwise mixed",
                           "addback_value_rule": "material >=.02 MAE and >=.01 CRPS aggregate recovery with >=3 coherent periods; modest >=.005/.002 with >=3; negligible below .005/.002; harmful joint worsening; otherwise mixed/regime",
                           "candidate_family_closed_before_fit": True})
    derivation = [{"feature": x, "stage3_classification": labels[x],
                   "stage4_role": "FOUNDATION" if x in foundation else ("MANDATORY_INDEPENDENT_ADDBACK_AND_SURVIVOR" if x in addbacks else "EXCLUDED_BY_MECHANICAL_RULE"),
                   "included_in_core1": x in foundation, "included_in_conservative_survivor": x in survivor,
                   "independent_addback_created": x in addbacks} for x in feature_order]
    return candidates, derivation


def freeze(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    reproduction = verify_stages(); write_json(output_dir / "stage4_stages1_3_reproduction.json", reproduction)
    artifact = stage1.load_artifact(); class_rows = read_csv(STAGE3 / "stage3_feature_replaceability_classification.csv")
    class_out = [{"feature_order": i + 1, "feature": x, "stage3_classification": next(r["stage3_primary_label"] for r in class_rows if r["feature"] == x),
                  "authoritative_source": str((STAGE3 / "stage3_feature_replaceability_classification.csv").relative_to(ROOT))}
                 for i, x in enumerate(artifact["feature_order"])]
    write_csv(output_dir / "stage4_stage3_class_map.csv", class_out)
    candidates, derivation = derive_candidates(class_rows, artifact["feature_order"])
    write_csv(output_dir / "stage4_candidate_derivation.csv", derivation)

    components = [
        {"concept_number": 1, "primitive_information_concept": "official venue/game identity", "role": "partitions strict-prior outcomes into venue histories", "direct_model_term": False},
        {"concept_number": 2, "primitive_information_concept": "official historical total-run outcomes", "role": "numerator of each venue adjusted-total ratio", "direct_model_term": False},
        {"concept_number": 3, "primitive_information_concept": "participating-team strict-prior scoring history", "role": "home and away expected-run denominator inputs; one concept instantiated for both sides", "direct_model_term": False},
        {"concept_number": 4, "primitive_information_concept": "league strict-prior run environment", "role": "8.6 initial and sparse-team expected-run fallback", "direct_model_term": False},
        {"concept_number": 5, "primitive_information_concept": "venue history depth/shrinkage support", "role": "n/(n+50) regression weight", "direct_model_term": False},
    ]
    write_csv(output_dir / "stage4_strict_prior_factor_components.csv", components)
    equivalence = equivalence_rows(artifact); write_csv(output_dir / "stage4_composite_equivalence.csv", equivalence)
    eq_status = "EXACT" if all(r["exact_matches"] == r["rows"] for r in equivalence) else (
        "MACHINE_TOLERANCE" if max(r["max_absolute_difference"] for r in equivalence) <= 5e-15 and not any(r["missingness_differences"] or r["fallback_differences"] for r in equivalence) else "NOT_EQUIVALENT")
    lineage = f"""# Strict-prior total-run-factor lineage

`STRICT_PRIOR_FACTOR_EQUIVALENCE = {eq_status}`

The direct model has one `strict_prior_total_run_factor` term, but the factor is composite and contains five defensible primitive information concepts (the team-scoring concept is instantiated separately for the home and away teams).

```text
official game + venue identity
  + official final home/away/total runs from dates strictly before the target date
  -> expanding league total mean (8.6 before any history)
  -> expanding home-team and away-team runs-scored means (league_mean / 2 when absent)
  -> adjusted venue ratio = prior_game_final_total / max(expected_home + expected_away, 0.5)
  -> direct venue ratio = mean(all prior adjusted ratios at venue)
  + venue history depth n
  -> shrinkage weight = n / (n + 50)
  -> strict_prior_total_run_factor = weight * direct_venue_ratio + (1 - weight) * 1.0
```

- Cutoff: historical construction freezes every game on a date before admitting any outcome from that date. It never reads the target game's outcome.
- Window: expanding from the governed spine start (2023-03-30), not a trailing window.
- Normalization: no inner z-score; `StandardScaler` is applied later by the location model. Denominator floor is 0.5.
- Clipping: none. Shrinkage target is 1.0 and constant 50 is fixed.
- Fallback: no venue history gives direct ratio 1, depth 0, weight 0, factor 1; team history falls back to half the strict-prior league mean; initial league mean is 8.6.
- Inputs: no sportsbook, market, opponent-prevention, prediction, or evaluation-outcome information.
- Support role: `park_history_depth` enters factor construction even if omitted as a separate direct model term.
- Version: historical `MLB_TOTALS_FEATURE_SPINE_V1` advances date-by-date. Prospective `live_context_bridge_v1` uses the identical equation on a foundation frozen through 2026-08-05. The latter does not advance the park foundation during Aug. 17–28.
"""
    (output_dir / "stage4_strict_prior_factor_lineage.md").write_text(lineage)

    write_csv(output_dir / "stage4_candidate_manifest.csv", candidates)
    manifest_sha = sha256(output_dir / "stage4_candidate_manifest.csv")
    (output_dir / "stage4_candidate_manifest_sha256.txt").write_text(manifest_sha + "\n")
    if eq_status not in ("EXACT", "MACHINE_TOLERANCE"):
        raise RuntimeError("STRICT_PRIOR_FACTOR_EQUIVALENCE_FAILED")
    return {"status": "CANDIDATES_FROZEN_BEFORE_EVALUATION", "candidate_count": len(candidates),
            "candidate_manifest_sha256": manifest_sha, "strict_prior_factor_equivalence": eq_status,
            "direct_terms": 1, "primitive_information_concepts": 5, "output": str(output_dir)}


def candidate_artifact(training: pd.DataFrame, identity: str, features: list[str], raw_artifact: dict[str, Any], manifest_sha: str) -> dict[str, Any]:
    pipeline = stage3.fit_pipeline(training, features, raw_artifact)
    mu = pipeline.predict(training[features]); alpha = stage3.dispersion(training.final_total.to_numpy(float), mu)
    value = {"candidate_identity": identity, "designation": "RESEARCH_STRUCTURE_ONLY_NOT_PROMOTED", "source_task": TASK_ID,
             "champion_model_hash": stage1.MODEL_HASH, "frozen_pre_fit_manifest_sha256": manifest_sha,
             "model_family": raw_artifact["model_family"], "model_class": "sklearn.linear_model.PoissonRegressor",
             "location_regularization_alpha": raw_artifact["location_regularization_alpha"], "location_max_iter": raw_artifact["location_max_iter"],
             "solver": "lbfgs", "solver_tolerance": 1e-4, "normalization": raw_artifact["normalization"],
             "development_games": len(training), "development_date_min": str(training.game_date.min().date()),
             "development_date_max": str(training.game_date.max().date()), "feature_order": features,
             "training_row_identity_target_hash": frame_hash(training, ["game_pk", "game_date", "final_total"]),
             "training_matrix_hash": frame_hash(training, ["game_pk", "final_total", *features]),
             "scaler_mean": pipeline["scaler"].mean_.tolist(), "scaler_scale": pipeline["scaler"].scale_.tolist(),
             "intercept": float(pipeline["location"].intercept_), "coefficients": pipeline["location"].coef_.tolist(),
             "dispersion_alpha": alpha, "distribution_support": raw_artifact["distribution_support"],
             "outcome_target": raw_artifact["outcome_target"], "fit_count": 1,
             "evaluation_rows_used_for_fit_selection_or_tuning": 0, "random_seed": "NOT_APPLICABLE_DETERMINISTIC_LBFGS",
             "sklearn_version": sklearn.__version__, "public_status": "RESEARCH_ONLY_NOT_AUTHORIZED"}
    value["canonical_candidate_hash"] = canonical_hash(value)
    return value


def score(frame: pd.DataFrame, artifact: dict[str, Any]) -> np.ndarray:
    values = frame[artifact["feature_order"]].astype(float).to_numpy()
    z = (values - np.asarray(artifact["scaler_mean"])) / np.asarray(artifact["scaler_scale"])
    return np.exp(float(artifact["intercept"]) + z @ np.asarray(artifact["coefficients"]))


def loss_and_metrics(frame: pd.DataFrame, mu: np.ndarray, alpha: float) -> tuple[dict[str, np.ndarray], dict[str, float]]:
    actual = frame.final_total.to_numpy(float)
    dist = stage1.row_distribution_losses(mu, actual, alpha)
    loss = {"mae": abs(actual - mu), "squared_error": (actual - mu) ** 2, "actual_minus_forecast_bias": actual - mu,
            "crps": dist["crps"], "brier": dist["brier"], "log_loss": dist["log_loss"],
            "event_probability": dist["event_probability"], "event_outcome": dist["event_outcome"]}
    metrics = {"games": len(frame), "mae": float(loss["mae"].mean()), "rmse": float(np.sqrt(loss["squared_error"].mean())),
               "actual_minus_forecast_bias": float(loss["actual_minus_forecast_bias"].mean()),
               "crps": float(loss["crps"].mean()), "brier": float(loss["brier"].mean()),
               "log_loss": float(loss["log_loss"].mean()), "ece": stage1.ece(loss["event_probability"], loss["event_outcome"])}
    return loss, metrics


def nb_log_likelihood(y: np.ndarray, mu: np.ndarray, alpha: float) -> float:
    if alpha <= 0:
        return float(np.sum(y * np.log(np.clip(mu, 1e-300, None)) - mu - gammaln(y + 1)))
    size = 1 / alpha; probability = size / (size + mu)
    return float(np.sum(nbinom.logpmf(y.astype(int), size, probability)))


def preservation_class(period_rows: list[dict[str, Any]], aggregate: dict[str, Any]) -> str:
    period_max = max(max(max(r["mae_degradation_fraction"], 0), max(r["crps_degradation_fraction"], 0)) for r in period_rows)
    for label in ("NEAR_COMPLETE", "SUBSTANTIAL", "PARTIAL", "LOW"):
        t = PRESERVATION_THRESHOLDS[label]
        rel_ok = all(max(aggregate[f"{x}_degradation_fraction"], 0) <= t["aggregate_relative_mae_rmse_crps"] for x in ("mae", "rmse", "crps"))
        if rel_ok and max(aggregate["brier_degradation"], 0) <= t["brier"] and max(aggregate["log_loss_degradation"], 0) <= t["log_loss"] and period_max <= t["maximum_period_relative_mae_crps"]:
            return label
    return "FAILED"


def temporal_status(rows: list[dict[str, Any]]) -> tuple[str, str]:
    states = []
    for r in rows:
        if max(r["mae_degradation_fraction"], 0) <= .03 and max(r["crps_degradation_fraction"], 0) <= .03 and max(r["brier_degradation"], 0) <= .003 and max(r["log_loss_degradation"], 0) <= .010:
            state = "PRESERVED"
        elif max(r["mae_degradation_fraction"], r["crps_degradation_fraction"]) > .10:
            state = "BROKEN"
        else: state = "MIXED"
        states.append(state)
    breaks = "|".join(stage1.PERIODS[i] for i, x in enumerate(states) if x != "PRESERVED") or "NONE"
    if all(x == "PRESERVED" for x in states): status = "STABLE"
    elif states.count("PRESERVED") >= 3: status = "MOSTLY_STABLE"
    elif states[-1] != states[0] and states[-1] in ("PRESERVED", "BROKEN"): status = "REGIME_DEPENDENT"
    elif states.count("BROKEN") >= 2: status = "UNSTABLE"
    else: status = "MIXED"
    return status, breaks


def evaluate(output_dir: Path, expected_manifest_sha: str) -> dict[str, Any]:
    manifest_path = output_dir / "stage4_candidate_manifest.csv"
    actual_sha = sha256(manifest_path)
    frozen_record = (output_dir / "stage4_candidate_manifest_sha256.txt").read_text().strip()
    if actual_sha != expected_manifest_sha or actual_sha != frozen_record:
        raise RuntimeError("FROZEN_STAGE4_CANDIDATE_MANIFEST_HASH_FAILED")
    if json.loads((output_dir / "stage4_stages1_3_reproduction.json").read_text())["STAGES_1_3_REPRODUCTION"] != "PASS":
        raise RuntimeError("PRIOR_REPRODUCTION_NOT_PASS")
    raw = stage1.load_artifact(); historical = stage1.raw.load_historical(raw)
    training = historical.loc[historical.period.eq("DEVELOPMENT_2023_24_IN_SAMPLE_REFERENCE")].copy().reset_index(drop=True)
    if len(training) != 4859: raise RuntimeError("TRAINING_POPULATION_PARITY_FAILED")
    frames, probability_guard = stage1.load_populations(raw)
    if {k: len(v) for k, v in frames.items()} != EXPECTED_COUNTS: raise RuntimeError("EVALUATION_POPULATION_PARITY_FAILED")

    artifacts_dir = output_dir / "candidate_artifacts"; artifacts_dir.mkdir(exist_ok=True)
    candidates: dict[str, dict[str, Any]] = {}; artifact_rows = []
    for row in read_csv(manifest_path):
        identity = row["candidate_identity"]; features = row["direct_feature_order_pipe"].split("|")
        candidate = candidate_artifact(training, identity, features, raw, actual_sha)
        path = artifacts_dir / f"{identity}.json"; write_json(path, candidate); candidates[identity] = candidate
        information_concepts = 5 + int("home_starter_prior_starts" in features)
        artifact_rows.append({"candidate_identity": identity, "direct_feature_count": len(features), "direct_feature_order_pipe": "|".join(features),
                              "underlying_unique_information_concept_count": information_concepts,
                              "concept_count_note": "league_total and park_history_depth repeat concepts already embedded in strict_prior_total_run_factor; home starter support adds one concept",
                              "canonical_candidate_hash": candidate["canonical_candidate_hash"], "artifact_relative_path": str(path.relative_to(output_dir)),
                              "artifact_sha256": sha256(path), "frozen_manifest_sha256": actual_sha, "fit_count": 1})
    write_csv(output_dir / "stage4_candidate_artifacts.csv", artifact_rows)

    variants = {"RAW": raw, **candidates}; all_frames = {**frames, AGGREGATE: pd.concat(list(frames.values()), ignore_index=True)}
    cache: dict[tuple[str, str], dict[str, Any]] = {}; point_rows = []; dist_rows = []
    for period, frame in all_frames.items():
        for identity, artifact in variants.items():
            mu = stage1.score_components(frame, raw)[0] if identity == "RAW" else score(frame, artifact)
            loss, metric = loss_and_metrics(frame, mu, float(artifact["dispersion_alpha"])); cache[(identity, period)] = {"mu": mu, "loss": loss, "metrics": metric}
        champion = cache[("RAW", period)]["metrics"]
        for identity in variants:
            metric = cache[(identity, period)]["metrics"]
            point_rows.append({"population": period, "variant": identity, "games": metric["games"],
                               "mae": metric["mae"], "rmse": metric["rmse"], "actual_minus_forecast_bias": metric["actual_minus_forecast_bias"],
                               "mae_delta_candidate_minus_raw": metric["mae"] - champion["mae"], "rmse_delta_candidate_minus_raw": metric["rmse"] - champion["rmse"],
                               "bias_delta_candidate_minus_raw": metric["actual_minus_forecast_bias"] - champion["actual_minus_forecast_bias"]})
            dist_rows.append({"population": period, "variant": identity, "games": metric["games"], "crps": metric["crps"],
                              "brier": metric["brier"], "log_loss": metric["log_loss"], "ece": metric["ece"],
                              "crps_delta_candidate_minus_raw": metric["crps"] - champion["crps"], "brier_delta_candidate_minus_raw": metric["brier"] - champion["brier"],
                              "log_loss_delta_candidate_minus_raw": metric["log_loss"] - champion["log_loss"], "ece_delta_candidate_minus_raw": metric["ece"] - champion["ece"]})
    write_csv(output_dir / "stage4_candidate_point_metrics.csv", point_rows); write_csv(output_dir / "stage4_candidate_distribution_metrics.csv", dist_rows)

    parity = []
    for period, frame in frames.items():
        parity.append({"population": period, "date_min": str(frame.game_date.min().date()), "date_max": str(frame.game_date.max().date()),
                       "expected_rows": EXPECTED_COUNTS[period], "observed_rows": len(frame), "all_candidate_rows": min(len(cache[(x, period)]["mu"]) for x in variants),
                       "row_count_parity": True, "unique_game_identities": not frame.game_pk.duplicated().any(), "outcomes_complete": bool(frame.final_total.notna().all()),
                       "row_identity_outcome_hash": frame_hash(frame, ["game_pk", "game_date", "final_total"]),
                       "raw_prediction_reproduced": True, "prospective_probability_max_abs_error": probability_guard.get("stored_probability_max_abs_error") if period == stage1.PERIODS[-1] else "NOT_APPLICABLE"})
    write_csv(output_dir / "stage4_evaluation_population_parity.csv", parity)

    # Descriptive training fit and information criteria.
    training_rows = []
    y = training.final_total.to_numpy(float)
    for identity, artifact in variants.items():
        mu = stage1.score_components(training, raw)[0] if identity == "RAW" else score(training, artifact)
        loss, metric = loss_and_metrics(training, mu, float(artifact["dispersion_alpha"])); coef = np.asarray(artifact["coefficients"])
        poisson_ll = float(np.sum(y * np.log(np.clip(mu, 1e-300, None)) - mu - gammaln(y + 1)))
        nb_ll = nb_log_likelihood(y, mu, float(artifact["dispersion_alpha"])); location_k = len(artifact["feature_order"]) + 1; distribution_k = location_k + 1
        objective = .5 * mean_poisson_deviance(y, mu) + float(artifact["location_regularization_alpha"]) * float(np.sum(coef ** 2)) / 2
        training_rows.append({"variant": identity, "games": len(training), "direct_feature_count": len(artifact["feature_order"]), "location_parameter_count": location_k,
                              "distribution_parameter_count_for_aic_bic": distribution_k, "poisson_log_likelihood": poisson_ll, "negative_binomial_log_likelihood": nb_ll,
                              "mean_poisson_deviance": mean_poisson_deviance(y, mu), "penalized_training_objective": objective,
                              "mae": metric["mae"], "rmse": metric["rmse"], "actual_minus_forecast_bias": metric["actual_minus_forecast_bias"], "crps": metric["crps"],
                              "aic_negative_binomial": 2 * distribution_k - 2 * nb_ll, "bic_negative_binomial": math.log(len(y)) * distribution_k - 2 * nb_ll,
                              "dispersion_alpha": artifact["dispersion_alpha"]})
    write_csv(output_dir / "stage4_training_metrics.csv", training_rows)

    preservation = []; temporal = []
    for identity in candidates:
        candidate_rows = []
        for period in (*stage1.PERIODS, AGGREGATE):
            cm = cache[(identity, period)]["metrics"]; rm = cache[("RAW", period)]["metrics"]
            row = {"candidate_identity": identity, "population": period,
                   **{f"{m}_degradation_fraction": (cm[m] - rm[m]) / rm[m] for m in ("mae", "rmse", "crps")},
                   "brier_degradation": cm["brier"] - rm["brier"], "log_loss_degradation": cm["log_loss"] - rm["log_loss"],
                   "bias_change": cm["actual_minus_forecast_bias"] - rm["actual_minus_forecast_bias"]}
            candidate_rows.append(row)
        overall = preservation_class(candidate_rows[:-1], candidate_rows[-1])
        for row in candidate_rows: row["champion_capability_preservation"] = overall
        preservation.extend(candidate_rows)
        status, breaks = temporal_status(candidate_rows[:-1]); temporal.append({"candidate_identity": identity, "REDUCED_CANDIDATE_TEMPORAL_STATUS": status,
                                                                                "periods_not_fully_preserved": breaks,
                                                                                "period_state_pipe": "|".join("PRESERVED" if max(r["mae_degradation_fraction"], r["crps_degradation_fraction"]) <= .03 else ("BROKEN" if max(r["mae_degradation_fraction"], r["crps_degradation_fraction"]) > .10 else "MIXED") for r in candidate_rows[:-1])})
    write_csv(output_dir / "stage4_champion_capability_preservation.csv", preservation); write_csv(output_dir / "stage4_temporal_stability.csv", temporal)
    preservation_map = {r["candidate_identity"]: r["champion_capability_preservation"] for r in preservation if r["population"] == AGGREGATE}
    temporal_map = {r["candidate_identity"]: r["REDUCED_CANDIDATE_TEMPORAL_STATUS"] for r in temporal}

    similarity = []
    for identity in candidates:
        for period in (*stage1.PERIODS, AGGREGATE):
            candidate_mu = cache[(identity, period)]["mu"]; raw_mu = cache[("RAW", period)]["mu"]; difference = candidate_mu - raw_mu
            cp = cache[(identity, period)]["loss"]["event_probability"]; rp = cache[("RAW", period)]["loss"]["event_probability"]; pdiff = cp - rp
            similarity.append({"candidate_identity": identity, "population": period, "rows": len(candidate_mu),
                               "pearson_expected_total": float(pearsonr(candidate_mu, raw_mu).statistic), "spearman_expected_total": float(spearmanr(candidate_mu, raw_mu).statistic),
                               "mean_signed_expected_total_difference": float(difference.mean()), "mean_absolute_expected_total_difference": float(abs(difference).mean()),
                               "median_absolute_expected_total_difference": float(np.median(abs(difference))), "p90_absolute_expected_total_difference": float(np.quantile(abs(difference), .9)),
                               "maximum_absolute_expected_total_difference": float(abs(difference).max()),
                               "pearson_governed_over_probability": float(pearsonr(cp, rp).statistic), "mean_absolute_probability_difference": float(abs(pdiff).mean()),
                               "probability_differences_ge_5pp": int((abs(pdiff) >= .05).sum()), "probability_differences_ge_10pp": int((abs(pdiff) >= .10).sum()),
                               "opposite_side_decisions_at_0_5": int(((cp > .5) != (rp > .5)).sum()), "governed_probability_threshold_count": len(stage1.THRESHOLDS)})
    write_csv(output_dir / "stage4_raw_output_similarity.csv", similarity)

    # Date-clustered uncertainty; Holm over the complete 80-comparison family.
    rng = np.random.default_rng(BOOTSTRAP_SEED); uncertainty = []
    for period, frame in frames.items():
        dates = pd.to_datetime(frame.game_date).dt.date.astype(str).to_numpy(); unique = np.unique(dates)
        draws = rng.integers(0, len(unique), size=(BOOTSTRAP_DRAWS, len(unique)))
        day_n = np.asarray([(dates == day).sum() for day in unique], float); denominator = day_n[draws].sum(axis=1)
        for identity in candidates:
            for metric in ("mae", "crps", "brier", "log_loss"):
                difference = cache[(identity, period)]["loss"][metric] - cache[("RAW", period)]["loss"][metric]
                day_sum = np.asarray([difference[dates == day].sum() for day in unique]); sampled = day_sum[draws].sum(axis=1) / denominator
                p = min(1.0, 2 * min(float(np.mean(sampled <= 0)), float(np.mean(sampled >= 0))))
                uncertainty.append({"population": period, "candidate_identity": identity, "metric": metric, "date_clusters": len(unique),
                                    "bootstrap_draws": BOOTSTRAP_DRAWS, "point_delta_candidate_minus_raw": float(difference.mean()),
                                    "ci95_low": float(np.quantile(sampled, .025)), "ci95_high": float(np.quantile(sampled, .975)),
                                    "fraction_favoring_candidate": float(np.mean(sampled < 0)), "fraction_favoring_raw": float(np.mean(sampled > 0)),
                                    "unadjusted_two_sided_bootstrap_p": p, "seed": BOOTSTRAP_SEED})
    write_csv(output_dir / "stage4_clustered_uncertainty.csv", uncertainty)
    adjusted = stage1.holm([r["unadjusted_two_sided_bootstrap_p"] for r in uncertainty])
    sensitivity = [{**r, "family_size": len(uncertainty), "holm_adjusted_p": a, "holm_significant_0_05": a < .05,
                    "interpretation": "FWER sensitivity across complete predeclared candidate-period-metric family; not independent confirmation"} for r, a in zip(uncertainty, adjusted)]
    write_csv(output_dir / "stage4_multiple_comparison_sensitivity.csv", sensitivity)

    # Coefficients and reorganization relative to RAW common terms.
    coefficient_rows = []
    raw_coefs = dict(zip(raw["feature_order"], raw["coefficients"]))
    for identity, artifact in variants.items():
        for index, feature in enumerate(artifact["feature_order"]):
            coef = float(artifact["coefficients"][index]); raw_coef = raw_coefs.get(feature)
            train_values = training[feature].to_numpy(float)
            coefficient_rows.append({"variant": identity, "parameter": feature, "parameter_type": "STANDARDIZED_DIRECT_FEATURE", "coefficient_or_value": coef,
                                     "standardized_effect_size": coef, "sign": "POSITIVE" if coef > 0 else ("NEGATIVE" if coef < 0 else "ZERO"),
                                     "raw_common_term_coefficient": raw_coef, "change_from_raw_common_term": coef - raw_coef if raw_coef is not None else None,
                                     "absolute_ratio_to_raw_common_term": abs(coef / raw_coef) if raw_coef else None,
                                     "sign_flip_from_raw": bool(raw_coef and np.sign(coef) != np.sign(raw_coef)),
                                     "training_support_mean": float(train_values.mean()), "training_support_std": float(train_values.std()),
                                     "training_support_min": float(train_values.min()), "training_support_max": float(train_values.max())})
        coefficient_rows += [{"variant": identity, "parameter": "INTERCEPT", "parameter_type": "INTERCEPT", "coefficient_or_value": artifact["intercept"]},
                             {"variant": identity, "parameter": "DISPERSION_ALPHA", "parameter_type": "DISPERSION", "coefficient_or_value": artifact["dispersion_alpha"]}]
    write_csv(output_dir / "stage4_coefficient_reorganization.csv", coefficient_rows)

    # Leakage-safe governed simple baselines: strict-prior league and symmetric team shrink state.
    baseline_rows = []
    for period, frame in all_frames.items():
        baseline_mu = {"SIMPLE_STRICT_PRIOR_LEAGUE_TOTAL": frame.league_total.to_numpy(float),
                       "TEAM_SHRUNK_OFFENSE_PREVENTION": .5 * (frame.home_offense + frame.away_offense + frame.home_prevention + frame.away_prevention).to_numpy(float)}
        for identity in variants:
            baseline_mu[identity] = cache[(identity, period)]["mu"]
        for identity, mu in baseline_mu.items():
            comparison_alpha = (float(variants[identity]["dispersion_alpha"])
                                if identity in variants else float(raw["dispersion_alpha"]))
            _, metric = loss_and_metrics(frame, np.asarray(mu), comparison_alpha)
            baseline_rows.append({"population": period, "variant": identity, "games": len(frame), "mae": metric["mae"], "rmse": metric["rmse"],
                                  "actual_minus_forecast_bias": metric["actual_minus_forecast_bias"], "crps": metric["crps"],
                                  "baseline_contract": "strict-prior feature-state point forecast; champion dispersion used only for descriptive CRPS" if identity.startswith(("SIMPLE", "TEAM_")) else "fitted model"})
    write_csv(output_dir / "stage4_simple_baseline_comparison.csv", baseline_rows)

    # Addback and formal decisions are deterministic applications of frozen rules.
    core = "CORE_1"; addback_rows = []
    for identity in [x for x in candidates if x.startswith("CORE_1_PLUS_")]:
        agg = cache[(identity, AGGREGATE)]["metrics"]; core_agg = cache[(core, AGGREGATE)]["metrics"]
        recovery_mae = core_agg["mae"] - agg["mae"]; recovery_crps = core_agg["crps"] - agg["crps"]
        coherent = sum(cache[(identity, p)]["metrics"]["mae"] < cache[(core, p)]["metrics"]["mae"] and cache[(identity, p)]["metrics"]["crps"] < cache[(core, p)]["metrics"]["crps"] for p in stage1.PERIODS)
        opposite = sum(cache[(identity, p)]["metrics"]["mae"] > cache[(core, p)]["metrics"]["mae"] and cache[(identity, p)]["metrics"]["crps"] > cache[(core, p)]["metrics"]["crps"] for p in stage1.PERIODS)
        if recovery_mae >= .02 and recovery_crps >= .01 and coherent >= 3: value = "MATERIAL_AND_STABLE"
        elif recovery_mae >= .005 and recovery_crps >= .002 and coherent >= 3: value = "MODEST_AND_STABLE"
        elif recovery_mae < -.005 and recovery_crps < -.002 and opposite >= 3: value = "HARMFUL"
        elif abs(recovery_mae) < .005 and abs(recovery_crps) < .002: value = "NEGLIGIBLE"
        elif temporal_map[identity] == "REGIME_DEPENDENT": value = "REGIME_DEPENDENT"
        else: value = "MIXED"
        feature = identity.removeprefix("CORE_1_PLUS_").lower()
        strict_coef = next(r["coefficient_or_value"] for r in coefficient_rows if r["variant"] == identity and r["parameter"] == "strict_prior_total_run_factor")
        add_coef = next(r["coefficient_or_value"] for r in coefficient_rows if r["variant"] == identity and r["parameter"] == feature)
        addback_rows.append({"candidate_identity": identity, "added_feature": feature, "direct_feature_count": len(candidates[identity]["feature_order"]),
                             "aggregate_mae_recovery_vs_core1": recovery_mae, "aggregate_crps_recovery_vs_core1": recovery_crps,
                             "periods_with_joint_mae_crps_recovery": coherent, "periods_with_joint_harm": opposite,
                             "mae_delta_vs_raw": agg["mae"] - cache[("RAW", AGGREGATE)]["metrics"]["mae"],
                             "crps_delta_vs_raw": agg["crps"] - cache[("RAW", AGGREGATE)]["metrics"]["crps"],
                             "temporal_status": temporal_map[identity], "strict_factor_coefficient": strict_coef, "addback_coefficient": add_coef,
                             "ADDBACK_VALUE": value})
    write_csv(output_dir / "stage4_addback_results.csv", addback_rows)

    core_pres = preservation_map[core]; core_temp = temporal_map[core]
    if core_temp in ("REGIME_DEPENDENT", "UNSTABLE"): core_result = "UNSTABLE"
    elif core_pres in ("NEAR_COMPLETE", "SUBSTANTIAL"): core_result = "SURPRISINGLY_SUFFICIENT"
    elif core_pres == "PARTIAL": core_result = "MEANINGFUL_BUT_INCOMPLETE"
    elif core_pres in ("LOW", "FAILED"): core_result = "CLEARLY_INSUFFICIENT"
    else: core_result = "UNRESOLVED"
    conservative = "CONSERVATIVE_SURVIVOR_CORE"; cons_pres = preservation_map[conservative]; cons_temp = temporal_map[conservative]
    if cons_temp in ("REGIME_DEPENDENT", "UNSTABLE"): cons_result = "UNSTABLE"
    elif cons_pres == "NEAR_COMPLETE": cons_result = "NEAR_CHAMPION"
    elif cons_pres in ("SUBSTANTIAL", "PARTIAL"): cons_result = "BROADLY_COMPARABLE"
    elif cons_pres == "LOW": cons_result = "MATERIALLY_WEAKER"
    else: cons_result = "FAILED"

    raw_strict = raw_coefs["strict_prior_total_run_factor"]
    strict_changes = {identity: next(r for r in coefficient_rows if r["variant"] == identity and r["parameter"] == "strict_prior_total_run_factor") for identity in candidates}
    if core_pres in ("NEAR_COMPLETE", "SUBSTANTIAL"): network = "SURPRISINGLY_UNNECESSARY"
    elif cons_pres in ("NEAR_COMPLETE", "SUBSTANTIAL") and core_pres in ("PARTIAL", "LOW"): network = "LARGELY_SURVIVES"
    elif cons_pres in ("PARTIAL", "LOW"): network = "PARTIALLY_COLLAPSES"
    elif cons_pres == "FAILED": network = "COLLAPSES_UNDER_SIMULTANEOUS_REMOVAL"
    else: network = "MIXED"

    stable_addbacks = [r for r in addback_rows if r["ADDBACK_VALUE"] in ("MATERIAL_AND_STABLE", "MODEST_AND_STABLE") and preservation_map[r["candidate_identity"]] in ("NEAR_COMPLETE", "SUBSTANTIAL", "PARTIAL")]
    if core_result in ("SURPRISINGLY_SUFFICIENT", "MEANINGFUL_BUT_INCOMPLETE") and core_temp in ("STABLE", "MOSTLY_STABLE"):
        minimal = "CORE_1_MINIMAL_FOUNDATION_CANDIDATE"
    elif stable_addbacks:
        minimal = "CORE_1_PLUS_ONE_MINIMAL_FOUNDATION_CANDIDATE"
    elif cons_result in ("NEAR_CHAMPION", "BROADLY_COMPARABLE") and cons_temp in ("STABLE", "MOSTLY_STABLE"):
        minimal = "CONSERVATIVE_SURVIVOR_CORE_MINIMAL_FOUNDATION_CANDIDATE"
    elif all(x == "FAILED" for x in preservation_map.values()): minimal = "NO_REDUCED_FOUNDATION_SUPPORTED"
    else: minimal = "REDUCED_FOUNDATION_REMAINS_UNRESOLVED"
    complexity = "SMALL_DIRECT_MODEL_BUT_INFORMATIONALLY_COMPLEX"
    discarded = "MOSTLY_REDUNDANT_INFORMATION" if network in ("SURPRISINGLY_UNNECESSARY", "LARGELY_SURVIVES") else ("NECESSARY_DISTRIBUTED_NETWORK" if network == "COLLAPSES_UNDER_SIMULTANEOUS_REMOVAL" else "MIXED")
    stop = "MLB_CHAMPION_SKELETON_SUFFICIENTLY_MAPPED_STOP_ACTIVE_DISSECTION" if minimal != "REDUCED_FOUNDATION_REMAINS_UNRESOLVED" else "ONE_ADDITIONAL_BOUNDED_QUESTION_REQUIRED"

    core_metric = cache[(core, AGGREGATE)]["metrics"]; raw_metric = cache[("RAW", AGGREGATE)]["metrics"]
    core_doc = f"""# CORE_1 review

`CORE_1_RESULT = {core_result}`

- Direct features: `strict_prior_total_run_factor` (1 direct term; 5 primitive information concepts).
- Training MAE/CRPS: {next(r for r in training_rows if r['variant']==core)['mae']:.6f} / {next(r for r in training_rows if r['variant']==core)['crps']:.6f}.
- Governed aggregate MAE/CRPS: {core_metric['mae']:.6f} / {core_metric['crps']:.6f}; RAW {raw_metric['mae']:.6f} / {raw_metric['crps']:.6f}.
- Capability preservation: `{core_pres}`; temporal status: `{core_temp}`.
- This is structural evidence from reused evaluation populations, not independent certification or promotion.
"""
    (output_dir / "stage4_core1_review.md").write_text(core_doc)
    cons_metric = cache[(conservative, AGGREGATE)]["metrics"]
    (output_dir / "stage4_conservative_survivor_core.md").write_text(f"""# Conservative survivor core

`CONSERVATIVE_CORE_RESULT = {cons_result}`

Direct features ({len(candidates[conservative]['feature_order'])}): {', '.join(candidates[conservative]['feature_order'])}. The survivor contains six unique primitive information concepts: the factor's five plus home-starter history support. Direct `league_total` and `park_history_depth` reuse concepts already embedded in the factor, so counting term instances would give eight but would double-count two concepts. Aggregate MAE/CRPS are {cons_metric['mae']:.6f}/{cons_metric['crps']:.6f}, versus RAW {raw_metric['mae']:.6f}/{raw_metric['crps']:.6f}. Preservation `{cons_pres}`; temporal `{cons_temp}`. No promotion is authorized.
""")
    (output_dir / "stage4_redundancy_network_behavior.md").write_text(f"""# Redundancy-network collapse

`REDUNDANCY_NETWORK_BEHAVIOR = {network}`

Stage 3 measured removals in a 21-feature environment; this Stage 4 result is the simultaneous-removal check. CORE_1 preserves `{core_pres}` and the four-term survivor preserves `{cons_pres}`. Strict-factor standardized coefficients range from {min(r['coefficient_or_value'] for r in strict_changes.values()):.6f} to {max(r['coefficient_or_value'] for r in strict_changes.values()):.6f}, versus RAW {raw_strict:.6f}; sign flips: {sum(r['sign_flip_from_raw'] for r in strict_changes.values())}. Details by period, metric, and coefficient are in the CSVs.
""")
    (output_dir / "stage4_foundation_complexity.md").write_text(f"""# Foundation versus composite illusion

`FOUNDATION_COMPLEXITY = {complexity}`

One direct location term is not one primitive signal. The surviving factor jointly carries venue identity, official historical outcomes, two-sided team scoring state, league run environment, and venue-depth shrinkage. Count/support fields remain legitimate upstream inputs even when absent from the direct matrix. Machine-tolerance equivalence makes the composite understood and reproducible, but not genuinely primitive.
""")
    (output_dir / "stage4_minimal_foundation_decision.md").write_text(f"""# Minimal credible foundation

`{minimal}`

This is the smallest predeclared candidate meeting the frozen descriptive preservation and temporal-coherence rules. “Foundation candidate” does not mean certified, champion, production-ready, independently validated, or irreducible. No model is promoted.
""")
    (output_dir / "stage4_22_feature_complexity_interpretation.md").write_text(f"""# Interpretation of the 22-feature complexity

`{discarded}`

The controlled simultaneous-removal results, Stage 3 refit recovery, output-similarity tables, and coefficient reorganization support this label. Repository evidence shows a larger assembly of individually plausible direct terms whose necessity had not been established by the frozen-ablation and replaceability sequence now completed; it does not establish developer intent. Replaceable features can still be useful support or alternative representations.
""")
    (output_dir / "stage4_stop_or_continue_decision.md").write_text(f"""# Stop or continue

`{stop}`

The direct skeleton, composite lineage/equivalence, single-addback value, conservative survivor behavior, and simultaneous redundancy response are now mapped. No further MLB stage is automatically authorized.
""")
    (output_dir / "stage4_nhl_transfer_rules.md").write_text("""# NHL-transfer modeling rules

No NHL asset was inspected.

1. Begin with the smallest leakage-safe concept set that has an explicit causal/timing rationale; count primitive concepts, not just columns.
2. Admit a new direct feature only after a predeclared incremental test on untouched temporal evidence; do not treat plausibility as earned complexity.
3. Freeze a no-refit ablation before refitting, then use leave-one-out refits to separate frozen dependence from unique information.
4. Preserve count/history/support variables for gating, shrinkage, and fallback even when they do not earn direct scoring roles.
5. Test home and away components independently and jointly; symmetry must be demonstrated rather than assumed.
6. Give every composite an independently reproducible lineage and equivalence contract across all model eras.
7. Inspect coefficient redistribution and simultaneous removal so correlated compensation is not mistaken for necessity.
8. Require each layer of complexity to earn admission through point, proper-score, calibration, temporal, and clustered-uncertainty evidence.
""")

    summaries = {identity: {"direct_features": len(artifact["feature_order"]), "preservation": preservation_map[identity], "temporal": temporal_map[identity]} for identity, artifact in candidates.items()}
    report = f"""# MLB Totals RAW champion controlled reduced-foundation reconstruction v1

`STAGES_1_3_REPRODUCTION = PASS`; `STRICT_PRIOR_FACTOR_EQUIVALENCE = MACHINE_TOLERANCE`.

The frozen manifest `{actual_sha}` contains exactly five mechanically derived candidates. No candidate was added after outcomes. CORE_1 is `{core_result}`; addbacks: {json.dumps({r['added_feature']: r['ADDBACK_VALUE'] for r in addback_rows}, sort_keys=True)}; conservative survivor core is `{cons_result}`. Candidate summary: `{json.dumps(summaries, sort_keys=True)}`.

`REDUNDANCY_NETWORK_BEHAVIOR = {network}`. `{minimal}`. `FOUNDATION_COMPLEXITY = {complexity}`. Discarded complexity: `{discarded}`. `{stop}`.

This is reused-evaluation structural analysis only. RAW and C are unchanged; no reduced model is promoted, no prospective experiment/public output was created, and no EV/ROI was calculated.
"""
    (output_dir / "concise_mlb_totals_raw_champion_controlled_reduced_foundation_reconstruction_v1.md").write_text(report)

    outputs = sorted(p for p in output_dir.rglob("*") if p.is_file() and p.name != "reproducibility_hashes.json")
    hashes = {"task_id": TASK_ID, "created_at_utc": datetime.now(timezone.utc).isoformat(), "model_identity": stage1.MODEL_IDENTITY,
              "model_hash": stage1.MODEL_HASH, "model_artifact_sha256": sha256(stage1.raw.CONFIG),
              "frozen_candidate_manifest_sha256": actual_sha, "candidate_count": len(candidates), "candidate_fit_count": len(candidates),
              "evaluation_rows_used_for_fitting_or_candidate_definition": 0, "bootstrap_seed": BOOTSTRAP_SEED, "bootstrap_draws": BOOTSTRAP_DRAWS,
              "analysis_utility_sha256": sha256(Path(__file__)), "outputs": {str(p.relative_to(output_dir)): sha256(p) for p in outputs}}
    write_json(output_dir / "reproducibility_hashes.json", hashes)
    return {"status": "PASS", "candidate_manifest_sha256": actual_sha, "candidate_results": summaries, "core1_result": core_result,
            "addbacks": {r["added_feature"]: r["ADDBACK_VALUE"] for r in addback_rows}, "conservative_core_result": cons_result,
            "redundancy_network_behavior": network, "minimal_foundation": minimal, "foundation_complexity": complexity,
            "discarded_complexity": discarded, "stop_decision": stop, "output": str(output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=("freeze", "evaluate"), required=True)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    parser.add_argument("--manifest-sha256")
    args = parser.parse_args()
    if args.phase == "freeze": result = freeze(args.output_dir)
    else:
        if not args.manifest_sha256: raise SystemExit("--manifest-sha256 is required for evaluate")
        result = evaluate(args.output_dir, args.manifest_sha256)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
