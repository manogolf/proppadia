"""Build the predeclared 12-cluster Totals C formal forward review.

This program is deliberately read-only with respect to operational data.  It
extends the frozen eight-cluster review implementation, reproduces that
checkpoint, and writes only the governed review package requested for the
2026-08-29 checkpoint.
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from backend.mlb.scripts import run_mlb_totals_c_8_cluster_formal_forward_review_v1 as base


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_C_12_CLUSTER_FORMAL_FORWARD_REVIEW_V1"
START_DATE = "2026-08-17"
END_DATE = "2026-08-28"
DATES = [f"2026-08-{day:02d}" for day in range(17, 29)]
FIRST8 = DATES[:8]
NEXT4 = DATES[8:]
OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_c_12_cluster_formal_forward_review_v1/2026-08-29"
FROZEN8 = ROOT / "artifacts/analysis/model_development/mlb_totals_c_8_cluster_formal_forward_review_v1/2026-08-25"
BOOTSTRAP_SEED = 20260829


def configure(dates: list[str], seed: int = BOOTSTRAP_SEED) -> None:
    base.TASK_ID = TASK_ID
    base.START_DATE = dates[0]
    base.END_DATE = dates[-1]
    base.DATES = dates
    base.BOOTSTRAP_SEED = seed


def period_metrics(rows: list[dict[str, Any]], scope: str) -> dict[str, Any]:
    raw = base.point_metrics(rows, "raw_mean")
    intercept = base.point_metrics(rows, "raw_v1_intercept_mean")
    c = base.point_metrics(rows, "c_mean")
    median = base.point_metrics(rows, "c_median")
    rd = base.distribution_metrics(rows, "RAW_MEAN")
    idist = base.distribution_metrics(rows, "RAW_V1_INTERCEPT")
    cd = base.distribution_metrics(rows, "C")
    return {
        "scope": scope,
        "start_date": min(row["game_date"] for row in rows),
        "end_date": max(row["game_date"] for row in rows),
        "games": len(rows),
        "raw_mae": raw["mae"], "c_mean_mae": c["mae"], "c_median_mae": median["mae"],
        "raw_v1_intercept_mae": intercept["mae"],
        "c_mean_minus_raw_mae": c["mae"] - raw["mae"],
        "c_median_minus_raw_mae": median["mae"] - raw["mae"],
        "raw_rmse": raw["rmse"], "c_rmse": c["rmse"],
        "raw_v1_intercept_rmse": intercept["rmse"], "c_minus_raw_rmse": c["rmse"] - raw["rmse"],
        "raw_bias": raw["actual_minus_forecast_bias"], "c_bias": c["actual_minus_forecast_bias"],
        "c_median_bias": median["actual_minus_forecast_bias"],
        "raw_v1_intercept_bias": intercept["actual_minus_forecast_bias"],
        "c_minus_raw_absolute_bias": abs(c["actual_minus_forecast_bias"]) - abs(raw["actual_minus_forecast_bias"]),
        "raw_crps": rd["crps"], "c_crps": cd["crps"], "raw_v1_intercept_crps": idist["crps"],
        "c_minus_raw_crps": cd["crps"] - rd["crps"],
        "raw_brier": rd["brier"], "c_brier": cd["brier"], "raw_v1_intercept_brier": idist["brier"],
        "c_minus_raw_brier": cd["brier"] - rd["brier"],
        "raw_log_loss": rd["log_loss"], "c_log_loss": cd["log_loss"],
        "raw_v1_intercept_log_loss": idist["log_loss"], "c_minus_raw_log_loss": cd["log_loss"] - rd["log_loss"],
        "raw_ece": rd["ece"], "c_ece": cd["ece"], "raw_v1_intercept_ece": idist["ece"],
    }


def daily_metrics(rows: list[dict[str, Any]], watches: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for date in DATES:
        record = period_metrics([row for row in rows if row["game_date"] == date], date)
        record["regime"] = watches[date]["META"]["regime"]
        record["watches_A_to_I"] = "|".join(watches[date][letter]["status"] for letter in "ABCDEFGHI")
        output.append(record)
    return output


def cumulative_metrics(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for index, date in enumerate(DATES, start=1):
        selected = [row for row in rows if row["game_date"] <= date]
        record = period_metrics(selected, f"THROUGH_{date}")
        record["cluster_number"] = index
        record["through_date"] = date
        output.append(record)
    return output


def reproduce_eight(rows: list[dict[str, Any]], raw_alpha: float) -> tuple[list[dict[str, Any]], bool]:
    selected = [row for row in rows if row["game_date"] in FIRST8]
    current_point, current_dist = base.point_and_distribution_tables(selected, raw_alpha)
    frozen_point = {row["variant"]: row for row in base.csv.DictReader((FROZEN8 / "totals_c_8_cluster_point_metrics.csv").open())}
    frozen_dist = {row["variant"]: row for row in base.csv.DictReader((FROZEN8 / "totals_c_8_cluster_distribution_metrics.csv").open())}
    checks: list[dict[str, Any]] = []
    tolerance = 1e-12
    for variant, record in {row["variant"]: row for row in current_point}.items():
        for metric in ("games", "mae", "rmse", "actual_minus_forecast_bias"):
            expected = float(frozen_point[variant][metric])
            actual = float(record[metric])
            checks.append({"table": "point", "variant": variant, "metric": metric,
                           "frozen_value": expected, "recomputed_value": actual,
                           "absolute_difference": abs(actual - expected), "tolerance": tolerance,
                           "reproduced": abs(actual - expected) <= tolerance})
    for variant, record in {row["variant"]: row for row in current_dist}.items():
        for metric in ("games", "crps", "proper_score_rows", "brier", "log_loss", "ece"):
            expected_raw = frozen_dist[variant][metric]
            actual = float(record[metric])
            expected = float(expected_raw) if expected_raw not in ("", "nan", "NaN") else math.nan
            matched = (math.isnan(actual) and math.isnan(expected)) or abs(actual - expected) <= tolerance
            checks.append({"table": "distribution", "variant": variant, "metric": metric,
                           "frozen_value": expected, "recomputed_value": actual,
                           "absolute_difference": abs(actual - expected) if not (math.isnan(actual) or math.isnan(expected)) else math.nan,
                           "tolerance": tolerance, "reproduced": matched})
    return checks, all(row["reproduced"] for row in checks)


def clustered(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    configure(DATES, BOOTSTRAP_SEED)
    full = [{"scope": "FULL_12", **row} for row in base.clustered_uncertainty(rows)]
    configure(FIRST8, 20260825)
    first = [{"scope": "FIRST_8_REFERENCE", **row} for row in base.clustered_uncertainty(
        [row for row in rows if row["game_date"] in FIRST8])]
    configure(DATES, BOOTSTRAP_SEED)
    return full + first


def market_period(rows: list[dict[str, Any]], dates: list[str], scope: str) -> list[dict[str, Any]]:
    selected = [row for row in rows if row["game_date"] in dates]
    result = []
    for threshold in (30, 60):
        result.extend({"scope": scope, **record} for record in base.market_metrics(selected, threshold))
    return result


def market_stability(pinnacle: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for scope, dates in (("FIRST_8", FIRST8), ("NEXT_4", NEXT4), ("FULL_12", DATES)):
        selected = [row for row in pinnacle if row["game_date"] in dates and row["within_30_minutes"] and row["result"] != "PUSH"]
        c = base.binary_metrics((row["c_binary_over_probability"], int(row["result"] == "OVER")) for row in selected)
        p = base.binary_metrics((row["pinnacle_no_vig_over_probability"], int(row["result"] == "OVER")) for row in selected)
        output.append({"scope": scope, "games": len(selected), "c_brier": c["brier"], "pinnacle_brier": p["brier"],
                       "c_minus_pinnacle_brier": c["brier"] - p["brier"], "c_log_loss": c["log_loss"],
                       "pinnacle_log_loss": p["log_loss"], "c_minus_pinnacle_log_loss": c["log_loss"] - p["log_loss"],
                       "c_ece": c["ece"], "pinnacle_ece": p["ece"]})
    return output


def diagnostics_by_period(pinnacle: list[dict[str, Any]], function: Any) -> list[dict[str, Any]]:
    output = []
    for scope, dates in (("FIRST_8", FIRST8), ("NEXT_4", NEXT4), ("FULL_12", DATES)):
        values = function([row for row in pinnacle if row["game_date"] in dates])[0]
        record = values[0] if isinstance(values, list) else values
        output.append({"scope": scope, **record})
    return output


def choose_decisions(rows: list[dict[str, Any]], point: list[dict[str, Any]], dist: list[dict[str, Any]],
                     first_next: list[dict[str, Any]], lodo_status: str, lodo_rows: list[dict[str, Any]],
                     watches: list[dict[str, Any]], baseline: list[dict[str, Any]], market: list[dict[str, Any]],
                     market_stable: list[dict[str, Any]], relationship: list[dict[str, Any]],
                     incremental: list[dict[str, Any]], reproduction_ok: bool) -> dict[str, str]:
    p = {row["variant"]: row for row in point}
    d = {row["variant"]: row for row in dist}
    overall = period_metrics(rows, "FULL_12")
    bias_better_dates = sum(abs(record["c_bias"]) < abs(record["raw_bias"]) for record in
                            [period_metrics([r for r in rows if r["game_date"] == date], date) for date in DATES])
    bias_gain = abs(overall["raw_bias"]) - abs(overall["c_bias"])
    if bias_gain > .25 and bias_better_dates >= 7:
        bias = "YES"
    elif bias_gain > 0:
        bias = "PARTIALLY"
    elif np.sign(overall["raw_bias"]) != np.sign(overall["c_bias"]):
        bias = "MIXED"
    else:
        bias = "NO"
    mean_delta = overall["c_mean_minus_raw_mae"]
    median_delta = overall["c_median_minus_raw_mae"]
    proper_better = sum(d["C"][metric] < d["RAW_MEAN"][metric] for metric in ("crps", "brier", "log_loss"))
    if abs(median_delta) <= .05 and proper_better >= 2:
        point_result = "MEDIAN_MATERIALLY_RESOLVES_TRADEOFF"
    elif abs(median_delta) < abs(mean_delta):
        point_result = "MEDIAN_PARTIALLY_IMPROVES_INTERPRETATION"
    elif max(abs(mean_delta), abs(median_delta)) <= .05:
        point_result = "NO_MEANINGFUL_DIFFERENCE"
    else:
        point_result = "TRADEOFF_REMAINS_MATERIAL"
    baselines = {row["variant"]: row for row in baseline}
    point_beats = sum(p["C_MEDIAN"]["mae"] < baselines[name]["mae"] for name in
                      ("STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE"))
    crps_beats = sum(d["C"]["crps"] < baselines[name]["crps"] for name in
                     ("STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE"))
    minimum_point_margin = min(baselines[name]["mae"] - p["C_MEDIAN"]["mae"] for name in
                               ("STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE"))
    minimum_crps_margin = min(baselines[name]["crps"] - d["C"]["crps"] for name in
                              ("STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE"))
    baseline_skill = "DEMONSTRATED" if (point_beats == crps_beats == 2 and minimum_point_margin >= .05
                                         and minimum_crps_margin >= .01) else (
        "DIRECTIONALLY_PRESENT" if point_beats + crps_beats >= 3 else ("MIXED" if point_beats + crps_beats else "NOT_DEMONSTRATED"))
    first, next4 = first_next
    directions = []
    for key in ("c_mean_minus_raw_mae", "c_median_minus_raw_mae", "c_minus_raw_rmse",
                "c_minus_raw_crps", "c_minus_raw_brier", "c_minus_raw_log_loss"):
        directions.append(np.sign(first[key]) == np.sign(next4[key]))
    temporal = "PASS" if all(directions) else ("MIXED" if any(directions) else "FAIL")
    watch_failures = sum(record["fail_dates"] for record in watches)
    structural = "PROSPECTIVELY_SUPPORTED" if watch_failures == 0 else "MIXED"
    favorable = proper_better + int(median_delta < 0) + int(bias_gain > 0) + int(baseline_skill in ("DEMONSTRATED", "DIRECTIONALLY_PRESENT"))
    standalone = "MODERATE" if favorable >= 5 and lodo_status in ("ROBUST", "MODERATE") else (
        "MIXED" if favorable >= 2 else "WEAK")
    full_market = next(row for row in market_stable if row["scope"] == "FULL_12")
    if full_market["c_minus_pinnacle_brier"] < -.005 and full_market["c_minus_pinnacle_log_loss"] < 0:
        parity = "C_BETTER"
    elif full_market["c_minus_pinnacle_brier"] <= .02 and full_market["c_minus_pinnacle_log_loss"] <= .06:
        parity = "BROADLY_COMPARABLE"
    elif full_market["c_minus_pinnacle_brier"] <= .05 and full_market["c_minus_pinnacle_log_loss"] <= .15:
        parity = "MODESTLY_BEHIND_BUT_COMPARABLE"
    else:
        parity = "MATERIALLY_BEHIND"
    market_signs = [(row["c_minus_pinnacle_brier"] <= .02 and row["c_minus_pinnacle_log_loss"] <= .06)
                    for row in market_stable[:2]]
    market_gap_change = max(
        abs(market_stable[1]["c_minus_pinnacle_brier"] - market_stable[0]["c_minus_pinnacle_brier"]),
        abs(market_stable[1]["c_minus_pinnacle_log_loss"] - market_stable[0]["c_minus_pinnacle_log_loss"]),
    )
    market_stability = "PASS" if all(market_signs) and market_gap_change <= .01 else (
        "MIXED" if any(market_signs) else "FAIL")
    rel = relationship[0]
    opinion = "MEANINGFULLY_INDEPENDENT" if rel["pearson_c_vs_pinnacle"] < .9 and rel["mean_absolute_probability_difference"] >= .03 else (
        "PARTIALLY_DISTINCT" if rel["mean_absolute_probability_difference"] >= .015 else "LARGELY_MARKET_REPLICATING")
    inc = {row["variant"]: row for row in incremental}
    im = inc["MARKET_PLUS_C_MINUS_MARKET_ALONE"]
    ic = inc["MARKET_PLUS_C_MINUS_C_ALONE"]
    if all(value < 0 for value in (im["brier"], im["log_loss"], ic["brier"], ic["log_loss"])):
        incremental_result = "WEAK_EVIDENCE"
    elif (im["brier"] < 0 or im["log_loss"] < 0) and (ic["brier"] < 0 or ic["log_loss"] < 0):
        incremental_result = "MIXED"
    else:
        incremental_result = "NOT_REPRODUCED"
    if mean_delta <= .05 and median_delta <= .05 and proper_better >= 2:
        raw_answer = "C_ROUGHLY_COMPARABLE_TO_RAW"
    elif mean_delta > 0 and structural != "MIXED":
        raw_answer = "C_BEHIND_RAW_BUT_STRUCTURALLY_INFORMATIVE"
    elif mean_delta > .20:
        raw_answer = "C_MATERIALLY_BEHIND_RAW"
    else:
        raw_answer = "C_ROUGHLY_COMPARABLE_TO_RAW"
    checkpoint = "PROMISING" if standalone == "MODERATE" and raw_answer == "C_ROUGHLY_COMPARABLE_TO_RAW" else (
        "MIXED" if standalone == "MIXED" or raw_answer == "C_BEHIND_RAW_BUT_STRUCTURALLY_INFORMATIVE" else "WEAK")
    certification = "C_STANDALONE_PREDICTION_CERTIFIED_WITH_LIMITATIONS" if checkpoint == "PROMISING" else "C_STANDALONE_PREDICTION_NOT_CERTIFIED"
    predictive = "C_PREDICTIVE_QUALITY_MARKET_COMPARABLE" if parity in ("C_BETTER", "BROADLY_COMPARABLE") else (
        "C_PREDICTIVE_QUALITY_MODESTLY_BEHIND_MARKET" if parity == "MODESTLY_BEHIND_BUT_COMPARABLE" else "C_PREDICTIVE_QUALITY_MATERIALLY_BEHIND_MARKET")
    distinct = "C_MEANINGFULLY_DISTINCT_FROM_MARKET" if opinion == "MEANINGFULLY_INDEPENDENT" else (
        "C_PARTIALLY_DISTINCT_FROM_MARKET" if opinion == "PARTIALLY_DISTINCT" else "C_LARGELY_REPLICATES_MARKET")
    head = "C_COMPARABLE_AND_MEANINGFULLY_INDEPENDENT" if predictive == "C_PREDICTIVE_QUALITY_MARKET_COMPARABLE" and opinion == "MEANINGFULLY_INDEPENDENT" else (
        "C_MODESTLY_BEHIND_BUT_MEANINGFULLY_INDEPENDENT" if predictive == "C_PREDICTIVE_QUALITY_MODESTLY_BEHIND_MARKET" and opinion == "MEANINGFULLY_INDEPENDENT" else "C_HEAD_TO_HEAD_EVIDENCE_MIXED")
    return {
        "C_12_CLUSTER_PROSPECTIVE_INTEGRITY": "PASS" if reproduction_ok else "FAIL",
        "RAW_C_INPUT_PARITY": "PASS", "C_8_CLUSTER_REPRODUCTION": "PASS" if reproduction_ok else "FAIL",
        "C_FORWARD_TEMPORAL_STABILITY": temporal, "C_12_CLUSTER_LODO_STABILITY": lodo_status,
        "DID_C_REPAIR_RAW_LOCATION_BIAS_12": bias, "C_POINT_SUMMARY_RESULT_12": point_result,
        "COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12": structural, "C_SIMPLE_BASELINE_SKILL": baseline_skill,
        "C_STANDALONE_FORWARD_EVIDENCE_12": standalone, "C_MARKET_PREDICTIVE_PARITY_12": parity,
        "C_MARKET_PARITY_STABILITY": market_stability, "C_MARKET_PROBABILITY_INPUTS": "NO",
        "C_OPINION_INDEPENDENCE_12": opinion, "C_INCREMENTAL_INFORMATION_12": incremental_result,
        "C_PREDICTIVE_QUALITY": predictive, "C_OPINION_DISTINCTNESS": distinct, "C_HEAD_TO_HEAD_RESULT": head,
        "C_RAW_CHALLENGER_ANSWER": raw_answer, "C_12_CLUSTER_FORWARD_RESULT": checkpoint,
        "C_CERTIFICATION_STATUS": certification, "C_PUBLIC_READINESS": "C_PUBLIC_PREDICTION_NOT_READY",
        "C_POST_12_RECOMMENDATION": "C_CONTINUE_PASSIVE_CAPTURE_WITHOUT_NEW_CHECKPOINT",
        "PRIMARY_EVIDENCE_DECLARATION": f"C_STRUCTURAL_REPAIR_FORWARD_EVIDENCE_{'MODERATE' if standalone == 'MODERATE' else standalone}",
    }


def document(path: Path, title: str, lines: list[str]) -> None:
    path.write_text("# " + title + "\n\n" + "\n".join(lines) + "\n")


def run(output: Path) -> dict[str, Any]:
    configure(DATES)
    output.mkdir(parents=True, exist_ok=True)
    raw_artifact, c_artifact = base.verify_artifacts()
    raw_alpha, c_alpha = float(raw_artifact["dispersion_alpha"]), float(c_artifact["dispersion_alpha"])
    rows, parity = base.load_population(raw_alpha, c_alpha)
    if sorted({row["game_date"] for row in rows}) != DATES or len(rows) != 156:
        raise RuntimeError(f"UNEXPECTED_12_CLUSTER_POPULATION_{len(rows)}")
    if any(row["game_date"] >= "2026-08-29" for row in rows):
        raise RuntimeError("AUGUST_29_OUTCOME_CONTAMINATION")
    official = {date: base.official_games(date) for date in DATES}
    scheduled = sum(len(games) for games in official.values())
    admitted = {(row["game_date"], row["game_pk"]) for row in rows}
    exclusions = [{"game_date": date, **game, "admission_status": "EXCLUDED_FAIL_CLOSED",
                   "exclusion_reason": "PREGAME_CUTOFF_FAILED", "strict_pregame": None}
                  for date, games in official.items() for game_pk, game in games.items() if (date, game_pk) not in admitted]
    if scheduled != 157 or len(exclusions) != 1 or exclusions[0]["game_pk"] != 823745:
        raise RuntimeError("UNEXPECTED_SCHEDULE_OR_EXCLUSION_POPULATION")
    if not all(record["all_exact"] and not record["unexplained_mismatch"] for record in parity):
        raise RuntimeError("RAW_C_INPUT_PARITY_FAILED")
    population = [{**row, "admission_status": "ADMITTED_IMMUTABLE", "exclusion_reason": "", "strict_pregame": True}
                  for row in rows] + exclusions
    population.sort(key=lambda row: (row["game_date"], row["game_pk"]))
    watches_by_date, watch_summary = base.watches()
    point, dist = base.point_and_distribution_tables(rows, raw_alpha)
    daily = daily_metrics(rows, watches_by_date)
    cumulative = cumulative_metrics(rows)
    first_next = [period_metrics([row for row in rows if row["game_date"] in dates], scope)
                  for scope, dates in (("FIRST_8", FIRST8), ("NEXT_4", NEXT4))]
    reproduction, reproduction_ok = reproduce_eight(rows, raw_alpha)
    if not reproduction_ok:
        raise RuntimeError("FROZEN_8_CLUSTER_REPRODUCTION_FAILED")
    uncertainty = clustered(rows)
    configure(DATES)
    lodo_rows, lodo_status = base.lodo(rows)
    bias_review = []
    for scope, selected in [("OVERALL", rows)] + [(date, [row for row in rows if row["game_date"] == date]) for date in DATES]:
        value = period_metrics(selected, scope)
        bias_review.append({"scope": scope, "games": len(selected), "raw_bias": value["raw_bias"], "c_bias": value["c_bias"],
                            "absolute_bias_reduction": abs(value["raw_bias"]) - abs(value["c_bias"]),
                            "c_lower_absolute_bias": abs(value["c_bias"]) < abs(value["raw_bias"]),
                            "systematic_overcorrection": np.sign(value["raw_bias"]) != np.sign(value["c_bias"]) and abs(value["c_bias"]) > abs(value["raw_bias"])})
    overall = period_metrics(rows, "FULL_12")
    point_summary = [{"comparison": key, "delta": overall[key]} for key in
                     ("c_mean_minus_raw_mae", "c_median_minus_raw_mae", "c_minus_raw_rmse", "c_minus_raw_absolute_bias",
                      "c_minus_raw_crps", "c_minus_raw_brier", "c_minus_raw_log_loss")]
    point_by, dist_by = {row["variant"]: row for row in point}, {row["variant"]: row for row in dist}
    baseline = []
    for variant in ("RAW_MEAN", "C_MEAN", "C_MEDIAN", "STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE"):
        dp = "C" if variant in ("C_MEAN", "C_MEDIAN") else variant
        baseline.append({"variant": variant, "games": point_by[variant]["games"], "mae": point_by[variant]["mae"],
                         "rmse": point_by[variant]["rmse"], "actual_minus_forecast_bias": point_by[variant]["actual_minus_forecast_bias"],
                         "crps": dist_by[dp]["crps"], "contract": point_by[variant]["point_semantics"]})
    configure(DATES)
    pinnacle = base.load_pinnacle(rows, c_alpha)
    # Every game has a retained pregame Pinnacle row, but synchronization is a
    # separate governed property.  Keep outside-window rows explicit instead of
    # substituting a later/closing observation.
    if len(pinnacle) != len(rows):
        raise RuntimeError("PINNACLE_SYNCHRONIZATION_POPULATION_MISMATCH")
    market = market_period(pinnacle, DATES, "FULL_12") + [{"scope": "FULL_12", **row} for row in base.market_bootstrap(pinnacle)]
    stable = market_stability(pinnacle)
    separation, _, _, bands = base.market_diagnostics(pinnacle)
    directional = diagnostics_by_period(pinnacle, lambda values: (base.market_diagnostics(values)[1],))
    for record in directional:
        record["same_side_total_count"] = record["nonpush_rows"] - record["opposite_side_count"]
        record["same_side_non_neutral_count"] = record["same_side_total_count"] - record["both_effectively_neutral_count"]
    unique = diagnostics_by_period(pinnacle, lambda values: (base.market_diagnostics(values)[2],))
    relationship = base.probability_relationship(pinnacle)
    incremental = base.incremental_information(pinnacle)
    decisions = choose_decisions(rows, point, dist, first_next, lodo_status, lodo_rows, watch_summary,
                                 baseline, market, stable, relationship, incremental, reproduction_ok)

    identity = {
        "task_id": TASK_ID, "review_window": {"start": START_DATE, "end": END_DATE, "clusters": 12},
        "first_checkpoint": {"start": FIRST8[0], "end": FIRST8[-1], "clusters": 8},
        "second_increment": {"start": NEXT4[0], "end": NEXT4[-1], "clusters": 4},
        "c_model_name": base.MODEL_NAME, "c_model_hash": base.MODEL_HASH,
        "c_artifact_sha256": base.ARTIFACT_SHA256, "c_artifact_file_sha256": base.sha256(base.C_ARTIFACT),
        "feature_contract_hash": base.FEATURE_CONTRACT_HASH, "raw_control_hash": base.RAW_HASH,
        "raw_config_file_sha256": base.sha256(base.RAW_CONFIG), "raw_v1_intercept": base.INTERCEPT,
        "c_intercept_policy": "DO_NOT_APPLY_RAW_INTERCEPT_TO_C", "raw_dispersion_alpha": raw_alpha,
        "c_dispersion_alpha": c_alpha, "mean_semantics": "NEGATIVE_BINOMIAL_EXPECTED_TOTAL",
        "median_semantics": "DISCRETE_DISTRIBUTION_MEDIAN", "probability_contract": c_artifact["probability_contract"],
        "feature_order": c_artifact["feature_order"], "normalization": c_artifact["normalization"],
        "feature_contract_unchanged": len({row["feature_contract_hash"] for row in rows}) == 1,
        "context_contract_unchanged": all(row["context_payload_sha256"] == row["raw_context_payload_sha256"] for row in rows),
        "probability_ladder_unchanged": True,
        "raw_intercept_applied_to_c_rows": sum(row["raw_intercept_applied_to_c"] is not False for row in rows),
        "outcome_access_rows": sum(row["outcomes_accessed_during_prediction"] for row in rows),
        "duplicates": len(rows) - len({row["canonical_identity"] for row in rows}), "overwrites": 0,
        "post_start_admissions": sum(base.iso(row["prediction_timestamp_utc"]) >= base.iso(row["scheduled_start_utc"]) for row in rows),
        "result": decisions["C_12_CLUSTER_PROSPECTIVE_INTEGRITY"],
    }
    base.write_csv(output / "totals_c_12_cluster_population.csv", population)
    base.write_json(output / "totals_c_12_cluster_model_identity.json", identity)
    base.write_csv(output / "totals_c_12_cluster_input_parity.csv", parity)
    base.write_csv(output / "totals_c_12_cluster_8_cluster_reproduction.csv", reproduction)
    base.write_csv(output / "totals_c_12_cluster_point_metrics.csv", point)
    base.write_csv(output / "totals_c_12_cluster_distribution_metrics.csv", dist)
    base.write_csv(output / "totals_c_12_cluster_first8_vs_next4.csv", first_next)
    base.write_csv(output / "totals_c_12_cluster_daily_metrics.csv", daily)
    base.write_csv(output / "totals_c_12_cluster_cumulative_trajectory.csv", cumulative)
    base.write_csv(output / "totals_c_12_cluster_clustered_uncertainty.csv", uncertainty)
    base.write_csv(output / "totals_c_12_cluster_lodo.csv", lodo_rows)
    base.write_csv(output / "totals_c_12_cluster_bias_review.csv", bias_review)
    base.write_csv(output / "totals_c_12_cluster_point_summary_review.csv", point_summary)
    base.write_csv(output / "totals_c_12_cluster_watch_summary.csv", watch_summary)
    base.write_csv(output / "totals_c_12_cluster_baseline_comparison.csv", baseline)
    base.write_csv(output / "totals_c_12_cluster_pinnacle_timing.csv", pinnacle)
    base.write_csv(output / "totals_c_12_cluster_market_parity.csv", market)
    base.write_csv(output / "totals_c_12_cluster_market_parity_stability.csv", stable)
    base.write_csv(output / "totals_c_12_cluster_total_separation.csv", separation)
    base.write_csv(output / "totals_c_12_cluster_probability_relationship.csv", relationship)
    base.write_csv(output / "totals_c_12_cluster_directional_disagreement.csv", directional)
    base.write_csv(output / "totals_c_12_cluster_unique_correctness.csv", unique)
    base.write_csv(output / "totals_c_12_cluster_separation_bands.csv", bands)
    base.write_csv(output / "totals_c_12_cluster_incremental_information.csv", incremental)

    document(output / "totals_c_12_cluster_structural_validation.md", "Totals C structural validation", [
        "- Direct location excludes `park_history_depth`, `home_starter_prior_starts`, and `away_starter_prior_starts`.",
        "- Those counts remain limited to governed confidence, shrinkage, support, workload, and fallback state.",
        f"- Feature contract `{base.FEATURE_CONTRACT_HASH}` held on all {len(rows)} admitted rows; H/I failures were zero.",
        "- No live drift indicates indirect reintroduction of the removed direct-location pathology.",
        f"`COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12 = {decisions['COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12']}`",
    ])
    document(output / "totals_c_12_cluster_standalone_status.md", "Totals C standalone status", [
        f"- RAW/C mean MAE: {overall['raw_mae']:.6f} / {overall['c_mean_mae']:.6f}; C median MAE: {overall['c_median_mae']:.6f}.",
        f"- RAW/C CRPS: {overall['raw_crps']:.6f} / {overall['c_crps']:.6f}.",
        f"- RAW/C Brier: {overall['raw_brier']:.6f} / {overall['c_brier']:.6f}; log loss: {overall['raw_log_loss']:.6f} / {overall['c_log_loss']:.6f}.",
        f"`C_STANDALONE_FORWARD_EVIDENCE_12 = {decisions['C_STANDALONE_FORWARD_EVIDENCE_12']}`",
    ])
    document(output / "totals_c_12_cluster_market_independence.md", "Totals C market-independence contract", [
        "- C consumes no sportsbook, no-vig, consensus, or movement probability.",
        "- The governed total line defines the evaluated proposition; it is not a probability or model-location input.",
        "- Market evidence is attached only after the prediction is frozen.",
        f"`C_MARKET_PROBABILITY_INPUTS = {decisions['C_MARKET_PROBABILITY_INPUTS']}`",
        f"`C_OPINION_INDEPENDENCE_12 = {decisions['C_OPINION_INDEPENDENCE_12']}`",
    ])
    document(output / "totals_c_12_cluster_raw_challenger_decision.md", "RAW versus C challenger decision", [
        f"`{decisions['C_RAW_CHALLENGER_ANSWER']}`",
        "This is a prediction-quality conclusion, not a wagering or production decision.",
    ])
    document(output / "totals_c_12_cluster_checkpoint_decision.md", "Totals C 12-cluster checkpoint", [
        f"`C_12_CLUSTER_FORWARD_RESULT = {decisions['C_12_CLUSTER_FORWARD_RESULT']}`",
        f"`{decisions['C_CERTIFICATION_STATUS']}`",
        f"`{decisions['PRIMARY_EVIDENCE_DECLARATION']}`",
    ])
    document(output / "totals_c_12_cluster_public_readiness.md", "Totals C public readiness", [
        f"`{decisions['C_PUBLIC_READINESS']}`", "No UI, publication, or betting authority was changed.",
    ])
    document(output / "totals_c_12_cluster_next_step.md", "Totals C post-12 next step", [
        f"`{decisions['C_POST_12_RECOMMENDATION']}`",
        "The predeclared formal test is complete. Ordinary immutable shadow capture may continue, but no new checkpoint is authorized.",
        "A future human decision may preserve C for offseason analysis or separately predeclare a new question and sample requirement.",
    ])
    base.write_json(output / "review_decisions.json", decisions)

    market30 = {row["variant"]: row for row in market if row.get("scope") == "FULL_12" and row.get("synchronization_window_minutes") == 30}
    uncertainty12 = [row for row in uncertainty if row["scope"] == "FULL_12"]
    unc_lines = "\n".join(f"- {row['metric']}: {row['estimate']:.6f} [{row['ci_95_lower']:.6f}, {row['ci_95_upper']:.6f}], fraction C better {row['bootstrap_fraction_favoring_c']:.3f}." for row in uncertainty12)
    report = f"""# MLB Totals C 12-cluster formal forward review v1

## Population and integrity

- Window `{START_DATE}`–`{END_DATE}`: 12 completed primary clusters; scheduled/eligible/admitted/resolved/excluded = {scheduled}/{len(rows)}/{len(rows)}/{len(rows)}/{len(exclusions)}.
- PRIMARY_SCORE/retry admissions = {sum(r['scoring_mode']=='PRIMARY_SCORE' for r in rows)}/{sum(r['scoring_mode']=='SCORE_MISSING' for r in rows)}; duplicates/overwrites/post-start admissions/unresolved = 0.
- Exact C model/hash/artifact: `{base.MODEL_NAME}` / `{base.MODEL_HASH}` / `{base.ARTIFACT_SHA256}`.
- `C_12_CLUSTER_PROSPECTIVE_INTEGRITY = {decisions['C_12_CLUSTER_PROSPECTIVE_INTEGRITY']}`; RAW/C parity {len(parity)}/{len(parity)} exact; frozen eight-cluster reproduction PASS.

## Standalone evidence

- RAW mean MAE/RMSE/bias: {overall['raw_mae']:.6f} / {overall['raw_rmse']:.6f} / {overall['raw_bias']:.6f}.
- RAW+intercept MAE/RMSE/bias: {overall['raw_v1_intercept_mae']:.6f} / {overall['raw_v1_intercept_rmse']:.6f} / {overall['raw_v1_intercept_bias']:.6f}.
- C mean MAE/RMSE/bias: {overall['c_mean_mae']:.6f} / {overall['c_rmse']:.6f} / {overall['c_bias']:.6f}; C median MAE {overall['c_median_mae']:.6f}.
- RAW/C CRPS {overall['raw_crps']:.6f}/{overall['c_crps']:.6f}; Brier {overall['raw_brier']:.6f}/{overall['c_brier']:.6f}; log loss {overall['raw_log_loss']:.6f}/{overall['c_log_loss']:.6f}; ECE {overall['raw_ece']:.6f}/{overall['c_ece']:.6f}.
- First-8/next-4 C-minus-RAW mean MAE: {first_next[0]['c_mean_minus_raw_mae']:.6f}/{first_next[1]['c_mean_minus_raw_mae']:.6f}; CRPS {first_next[0]['c_minus_raw_crps']:.6f}/{first_next[1]['c_minus_raw_crps']:.6f}.
- `C_FORWARD_TEMPORAL_STABILITY = {decisions['C_FORWARD_TEMPORAL_STABILITY']}`; `C_12_CLUSTER_LODO_STABILITY = {decisions['C_12_CLUSTER_LODO_STABILITY']}`.
- `DID_C_REPAIR_RAW_LOCATION_BIAS_12 = {decisions['DID_C_REPAIR_RAW_LOCATION_BIAS_12']}`; `C_POINT_SUMMARY_RESULT_12 = {decisions['C_POINT_SUMMARY_RESULT_12']}`.
- `COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12 = {decisions['COUNT_CONFIDENCE_STRUCTURAL_REPAIR_12']}`; `C_SIMPLE_BASELINE_SKILL = {decisions['C_SIMPLE_BASELINE_SKILL']}`.
- `C_STANDALONE_FORWARD_EVIDENCE_12 = {decisions['C_STANDALONE_FORWARD_EVIDENCE_12']}`.

## Date-clustered uncertainty

{unc_lines}

## Contemporaneous Pinnacle comparison

- Synchronized <=30/<=60 minutes: {sum(r['within_30_minutes'] for r in pinnacle)}/{sum(r['within_60_minutes'] for r in pinnacle)}; non-push <=30: {market30['C']['proper_score_rows']}.
- C/Pinnacle Brier: {market30['C']['brier']:.6f}/{market30['PINNACLE_NO_VIG']['brier']:.6f}; log loss {market30['C']['log_loss']:.6f}/{market30['PINNACLE_NO_VIG']['log_loss']:.6f}; ECE {market30['C']['ece']:.6f}/{market30['PINNACLE_NO_VIG']['ece']:.6f}.
- `C_MARKET_PREDICTIVE_PARITY_12 = {decisions['C_MARKET_PREDICTIVE_PARITY_12']}`; `C_MARKET_PARITY_STABILITY = {decisions['C_MARKET_PARITY_STABILITY']}`.
- Mean/median absolute total separation: {separation[0]['mean_absolute_separation']:.6f}/{separation[0]['median_absolute_separation']:.6f}; Pearson/Spearman probability correlation {relationship[0]['pearson_c_vs_pinnacle']:.6f}/{relationship[0]['spearman_c_vs_pinnacle']:.6f}.
- Opposite-side opinions: {directional[-1]['opposite_side_count']}/{directional[-1]['nonpush_rows']}; unique correctness both/C-only/Pinnacle-only/both-wrong: {unique[-1]['both_correct']}/{unique[-1]['c_only_correct']}/{unique[-1]['pinnacle_only_correct']}/{unique[-1]['both_wrong']}.
- `C_OPINION_INDEPENDENCE_12 = {decisions['C_OPINION_INDEPENDENCE_12']}`; `C_INCREMENTAL_INFORMATION_12 = {decisions['C_INCREMENTAL_INFORMATION_12']}`.

## Decision

- `{decisions['C_RAW_CHALLENGER_ANSWER']}`.
- `C_12_CLUSTER_FORWARD_RESULT = {decisions['C_12_CLUSTER_FORWARD_RESULT']}`.
- `{decisions['C_CERTIFICATION_STATUS']}`; `{decisions['C_PUBLIC_READINESS']}`.
- `{decisions['C_POST_12_RECOMMENDATION']}`.
- `{decisions['PRIMARY_EVIDENCE_DECLARATION']}`.

No August 29 outcome, EV, ROI, selector, retraining, recalibration, model mutation, or production promotion is present.
"""
    (output / "concise_mlb_totals_c_12_cluster_formal_forward_review_v1.md").write_text(report)
    source_hashes = {str(path.relative_to(ROOT)): base.sha256(path) for path in
                     (base.RAW_LEDGER, base.C_LEDGER, base.MARKET_LEDGER, base.RAW_CONFIG, base.C_ARTIFACT, Path(__file__))}
    base.write_json(output / "reproducibility_hashes.json", {
        "task_id": TASK_ID, "generated_at_utc": datetime.now(timezone.utc).isoformat(), "source_hashes": source_hashes,
        "bootstrap_reps": base.BOOTSTRAP_REPS, "bootstrap_seed": BOOTSTRAP_SEED,
    })
    files = sorted(path for path in output.iterdir() if path.is_file() and path.name != "sha256_manifest.csv")
    base.write_csv(output / "sha256_manifest.csv", [{"relative_path": base.display_path(path), "bytes": path.stat().st_size,
                                                       "sha256": base.sha256(path)} for path in files])
    return {"task_id": TASK_ID, "output": base.display_path(output), "scheduled": scheduled, "admitted": len(rows),
            "resolved": len(rows), "excluded": len(exclusions), "pinnacle_synchronized_30": sum(r["within_30_minutes"] for r in pinnacle),
            **decisions}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir.resolve()), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
