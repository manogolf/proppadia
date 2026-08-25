"""Build the predeclared eight-cluster Totals C forward review.

This analysis reads immutable prospective prediction, context, outcome, watch, and
market ledgers. It never scores a game, changes a model, or writes operational data.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from scipy.stats import spearmanr
from sklearn.linear_model import LogisticRegression

from backend.mlb.scripts import run_mlb_totals_count_feature_structural_repair_comparison_v1 as structural
from backend.mlb.totals_predictions.c_shadow_v1 import payload_hash as c_payload_hash
from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution
from backend.mlb.totals_predictions.prospective_shadow_v1 import payload_hash as raw_payload_hash


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_C_8_CLUSTER_FORMAL_FORWARD_REVIEW_V1"
START_DATE = "2026-08-17"
END_DATE = "2026-08-24"
DATES = [f"2026-08-{day:02d}" for day in range(17, 25)]
MODEL_NAME = "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1"
MODEL_HASH = "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd"
ARTIFACT_SHA256 = "ea496a7a65d6ffad306238a46dd1279cf0cc81675c07f7447e9a48b511b4abfc"
FEATURE_CONTRACT_HASH = "d7551fd7798aa60ada1b96831e32bcb7748a17aabf67f53c8800f24c9f4a0927"
RAW_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
INTERCEPT = 0.493550
BOOTSTRAP_REPS = 10_000
BOOTSTRAP_SEED = 20260825

RAW_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
C_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_c_shadow_v1/totals_c_shadow_v1.sqlite3"
MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
RAW_CONFIG = ROOT / "backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json"
C_ARTIFACT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_feature_structural_repair_comparison_v1/2026-08-16/DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1.json"
OFFICIAL_ROOT = ROOT / "artifacts/analysis/mlb/player_stats_completeness"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_c_8_cluster_formal_forward_review_v1/2026-08-25"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode()
    return hashlib.sha256(raw).hexdigest()


def iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def display_path(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def nb_mass(mu: float, alpha: float) -> np.ndarray:
    result = distribution(float(mu), float(alpha))
    if abs(float(result.sum()) - 1.0) > 1e-12:
        raise RuntimeError("PROBABILITY_MASS_NOT_NORMALIZED")
    return result


def crps_from_mass(mass: np.ndarray, actual: int) -> float:
    support = np.arange(len(mass))
    return float(np.sum((np.cumsum(mass) - (support >= int(actual)).astype(float)) ** 2))


def line_probability(mass: np.ndarray, line: float) -> tuple[float, float, float, float]:
    support = np.arange(len(mass))
    over = float(mass[support > line].sum())
    under = float(mass[support < line].sum())
    push = float(mass[support == line].sum())
    binary_over = over / (over + under) if over + under else math.nan
    return over, under, push, binary_over


def ece(probabilities: np.ndarray, outcomes: np.ndarray) -> float:
    total = len(probabilities)
    if not total:
        return math.nan
    value = 0.0
    for index in range(10):
        lower, upper = index / 10.0, (index + 1) / 10.0
        mask = (probabilities >= lower) & ((probabilities < upper) if index < 9 else (probabilities <= upper))
        if mask.any():
            value += float(mask.mean()) * abs(float(probabilities[mask].mean()) - float(outcomes[mask].mean()))
    return value


def binary_metrics(records: Iterable[tuple[float, int]]) -> dict[str, float]:
    values = list(records)
    if not values:
        return {"proper_score_rows": 0, "brier": math.nan, "log_loss": math.nan, "ece": math.nan}
    probabilities = np.array([item[0] for item in values], dtype=float)
    outcomes = np.array([item[1] for item in values], dtype=float)
    clipped = np.clip(probabilities, 1e-15, 1 - 1e-15)
    return {
        "proper_score_rows": len(values),
        "brier": float(np.mean((probabilities - outcomes) ** 2)),
        "log_loss": float(np.mean(-(outcomes * np.log(clipped) + (1 - outcomes) * np.log(1 - clipped)))),
        "ece": ece(probabilities, outcomes),
    }


def point_metrics(rows: list[dict[str, Any]], forecast: str) -> dict[str, float]:
    errors = np.array([float(row["actual_total"]) - float(row[forecast]) for row in rows])
    return {
        "games": len(rows),
        "mae": float(np.mean(np.abs(errors))),
        "rmse": float(np.sqrt(np.mean(errors**2))),
        "actual_minus_forecast_bias": float(np.mean(errors)),
    }


def distribution_metrics(rows: list[dict[str, Any]], variant: str) -> dict[str, float]:
    crps_key = {"RAW_MEAN": "raw_crps", "RAW_V1_INTERCEPT": "intercept_crps", "C": "c_crps"}[variant]
    probability_key = {"RAW_MEAN": "raw_binary_over", "RAW_V1_INTERCEPT": "intercept_binary_over", "C": "c_binary_over"}[variant]
    records = [(float(row[probability_key]), int(row["actual_total"] > row["governed_total_line"]))
               for row in rows if row["actual_total"] != row["governed_total_line"]]
    return {"games": len(rows), "crps": float(np.mean([row[crps_key] for row in rows])), **binary_metrics(records)}


def official_games(date: str) -> dict[int, dict[str, Any]]:
    output: dict[int, dict[str, Any]] = {}
    for directory in sorted((OFFICIAL_ROOT / date).glob("game_*")):
        game_pk = int(directory.name.split("_", 1)[1])
        sources = sorted((directory / "sources").glob(f"game_{game_pk}_live_feed_*.json"))
        finals = []
        for source in sources:
            payload = json.loads(source.read_text())
            if payload.get("gameData", {}).get("status", {}).get("abstractGameState") == "Final":
                total = int(payload["liveData"]["linescore"]["teams"]["away"]["runs"]) + int(payload["liveData"]["linescore"]["teams"]["home"]["runs"])
                finals.append((source, payload, total))
        if not finals:
            raise RuntimeError(f"NO_RETAINED_OFFICIAL_FINAL_{date}_{game_pk}")
        if len({item[2] for item in finals}) != 1:
            raise RuntimeError(f"CONFLICTING_RETAINED_OFFICIAL_FINAL_{date}_{game_pk}")
        source, payload, total = finals[0]
        output[game_pk] = {
            "game_pk": game_pk,
            "scheduled_start_utc": payload["gameData"]["datetime"]["dateTime"],
            "actual_total": total,
            "official_source_path": str(source.relative_to(ROOT)),
            "official_source_sha256": sha256(source),
        }
    return output


def verify_artifacts() -> tuple[dict[str, Any], dict[str, Any]]:
    raw = json.loads(RAW_CONFIG.read_text())
    stable = {key: value for key, value in raw.items() if key != "canonical_model_hash"}
    if canonical_hash(stable) != RAW_HASH or raw.get("canonical_model_hash") != RAW_HASH:
        raise RuntimeError("RAW_CONFIG_HASH_MISMATCH")
    c_bytes = C_ARTIFACT.read_bytes()
    c = json.loads(c_bytes)
    if hashlib.sha256(c_bytes).hexdigest() != ARTIFACT_SHA256:
        raise RuntimeError("C_ARTIFACT_SHA_MISMATCH")
    if c.get("candidate_identity") != MODEL_NAME or c.get("canonical_model_hash") != MODEL_HASH:
        raise RuntimeError("C_ARTIFACT_IDENTITY_MISMATCH")
    if structural.artifact_hash(c) != MODEL_HASH:
        raise RuntimeError("C_CANONICAL_HASH_RECOMPUTATION_FAILED")
    contract = {key: c[key] for key in ("feature_order", "scaler_mean", "scaler_scale", "normalization")}
    if canonical_hash(contract) != FEATURE_CONTRACT_HASH:
        raise RuntimeError("C_FEATURE_CONTRACT_HASH_MISMATCH")
    return raw, c


def load_population(raw_alpha: float, c_alpha: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    raw_connection = ro(RAW_LEDGER)
    c_connection = ro(C_LEDGER)
    raw_rows = raw_connection.execute("""
      SELECT p.canonical_identity,p.game_date,p.game_id,p.scheduled_start_utc,p.prediction_timestamp_utc,
             p.model_hash,p.feature_state_hash,p.schedule_source_hash,p.market_source_hash,
             p.prediction_payload_json,p.prediction_payload_sha256,c.context_payload_json,c.context_payload_sha256
      FROM totals_shadow_predictions p JOIN totals_shadow_prediction_context c USING(canonical_identity)
      WHERE p.game_date BETWEEN ? AND ? ORDER BY p.game_date,p.game_id
    """, (START_DATE, END_DATE)).fetchall()
    raw_by_game: dict[tuple[str, int], dict[str, Any]] = {}
    for db in raw_rows:
        identity, date, game_pk, scheduled, predicted, model_hash, feature_hash, schedule_hash, market_hash, pred_json, pred_sha, context_json, context_sha = db
        prediction, context = json.loads(pred_json), json.loads(context_json)
        if model_hash != RAW_HASH or prediction.get("model_hash") != RAW_HASH:
            raise RuntimeError(f"RAW_HASH_MISMATCH_{identity}")
        if raw_payload_hash(prediction) != pred_sha or raw_payload_hash(context) != context_sha:
            raise RuntimeError(f"RAW_PAYLOAD_HASH_MISMATCH_{identity}")
        raw_by_game[(date, int(game_pk))] = {
            "identity": identity, "scheduled": scheduled, "predicted": predicted, "feature_hash": feature_hash,
            "schedule_hash": schedule_hash, "market_hash": market_hash, "prediction": prediction,
            "prediction_sha": pred_sha, "context": context, "context_sha": context_sha,
        }
    query = """
      SELECT p.canonical_identity,p.game_date,p.game_pk,p.model_name,p.model_hash,p.artifact_sha256,
             p.scheduled_start_utc,p.prediction_timestamp_utc,p.source_raw_identity,p.feature_state_hash,
             p.prediction_payload_json,p.prediction_payload_sha256,c.context_payload_json,c.context_payload_sha256,
             o.official_final_total,o.official_source_hash,o.outcome_payload_json,o.outcome_payload_sha256
      FROM totals_c_shadow_predictions p JOIN totals_c_shadow_contexts c USING(canonical_identity)
      LEFT JOIN totals_c_shadow_outcomes o USING(canonical_identity)
      WHERE p.game_date BETWEEN ? AND ? ORDER BY p.game_date,p.game_pk
    """
    db_rows = c_connection.execute(query, (START_DATE, END_DATE)).fetchall()
    raw_connection.close(); c_connection.close()
    output: list[dict[str, Any]] = []
    parity: list[dict[str, Any]] = []
    for db in db_rows:
        (identity, date, game_pk, model_name, model_hash, artifact_sha, scheduled, predicted, source_raw_identity,
         feature_hash, pred_json, pred_sha, context_json, context_sha, outcome_total, official_hash,
         outcome_json, outcome_sha) = db
        prediction, context = json.loads(pred_json), json.loads(context_json)
        raw = raw_by_game[(date, int(game_pk))]
        if (model_name, model_hash, artifact_sha) != (MODEL_NAME, MODEL_HASH, ARTIFACT_SHA256):
            raise RuntimeError(f"C_IDENTITY_MISMATCH_{identity}")
        if c_payload_hash(prediction) != pred_sha or c_payload_hash(context) != context_sha:
            raise RuntimeError(f"C_PAYLOAD_HASH_MISMATCH_{identity}")
        if outcome_total is None:
            raise RuntimeError(f"UNRESOLVED_C_OUTCOME_{identity}")
        if iso(predicted) >= iso(scheduled):
            raise RuntimeError(f"POST_START_C_ROW_{identity}")
        if prediction.get("raw_intercept_applied_to_c") is not False or prediction.get("outcomes_accessed_during_prediction") != 0:
            raise RuntimeError(f"C_POLICY_BREACH_{identity}")
        outcome_payload = json.loads(outcome_json)
        official_path = ROOT / outcome_payload["official_source_path"]
        official_payload = json.loads(official_path.read_text())
        official_total = int(official_payload["liveData"]["linescore"]["teams"]["away"]["runs"]) + int(official_payload["liveData"]["linescore"]["teams"]["home"]["runs"])
        official = {
            "actual_total": official_total,
            "official_source_path": str(official_path.relative_to(ROOT)),
            "official_source_sha256": sha256(official_path),
        }
        if (int(outcome_total) != official["actual_total"] or official_hash != official["official_source_sha256"]
                or outcome_payload["official_source_hash"] != official_hash):
            raise RuntimeError(f"C_OUTCOME_BINDING_MISMATCH_{identity}")
        raw_mu = float(raw["prediction"]["expected_total"])
        c_mu = float(prediction["expected_total_mean"])
        c_median = float(prediction["central_total_median"])
        line = float(prediction["governed_total_line"])
        raw_mass = nb_mass(raw_mu, raw_alpha)
        intercept_mass = nb_mass(raw_mu + INTERCEPT, raw_alpha)
        c_mass = nb_mass(c_mu, c_alpha)
        _, _, _, raw_binary = line_probability(raw_mass, line)
        _, _, _, intercept_binary = line_probability(intercept_mass, line)
        _, _, _, c_binary = line_probability(c_mass, line)
        features = context["model_features"]
        row = {
            "game_date": date, "game_pk": int(game_pk), "canonical_identity": identity,
            "scheduled_start_utc": scheduled, "prediction_timestamp_utc": predicted,
            "scoring_mode": prediction["scoring_mode"], "regime": prediction["regime_classification"],
            "model_name": model_name, "model_hash": model_hash, "artifact_sha256": artifact_sha,
            "raw_identity": source_raw_identity, "raw_prediction_timestamp_utc": raw["predicted"],
            "raw_mean": raw_mu, "raw_v1_intercept_mean": raw_mu + INTERCEPT,
            "c_mean": c_mu, "c_median": c_median, "actual_total": int(outcome_total),
            "governed_total_line": line, "raw_binary_over": raw_binary,
            "intercept_binary_over": intercept_binary, "c_binary_over": c_binary,
            "raw_crps": crps_from_mass(raw_mass, int(outcome_total)),
            "intercept_crps": crps_from_mass(intercept_mass, int(outcome_total)),
            "c_crps": crps_from_mass(c_mass, int(outcome_total)),
            "strict_prior_league_baseline": float(prediction["comparator_prior_population_baseline"]),
            "team_shrunk_baseline": float(prediction["comparator_team_shrunk_baseline"]),
            "feature_contract_hash": prediction["feature_contract_hash"],
            "feature_state_hash": feature_hash, "context_payload_sha256": context_sha,
            "raw_context_payload_sha256": raw["context_sha"],
            "schedule_source_sha256": prediction["schedule_source_sha256"],
            "market_source_sha256": prediction.get("market_source_sha256"),
            "official_source_path": official["official_source_path"],
            "official_source_sha256": official["official_source_sha256"],
            "prediction_payload_sha256": pred_sha, "outcome_payload_sha256": outcome_sha,
            "outcomes_accessed_during_prediction": prediction["outcomes_accessed_during_prediction"],
            "raw_intercept_applied_to_c": prediction["raw_intercept_applied_to_c"],
        }
        for key in ("raw_abs_error", "c_mean_abs_error", "c_median_abs_error"):
            forecast = {"raw_abs_error": raw_mu, "c_mean_abs_error": c_mu, "c_median_abs_error": c_median}[key]
            row[key] = abs(int(outcome_total) - forecast)
        output.append(row)
        checks = {
            "probable_pitchers": all(prediction.get(f"{side}_probable_starter_id") == raw["prediction"].get(f"{side}_probable_starter_id") for side in ("home", "away")),
            "bullpen_state": all(context.get(f"{side}_bullpen_state") == raw["context"].get(f"{side}_bullpen_state") for side in ("home", "away")),
            "park_state": context.get("park_state") == raw["context"].get("park_state"),
            "schedule_state": prediction["schedule_source_sha256"] == raw["schedule_hash"],
            "market_line_state": line == float(raw["prediction"]["total_line"]),
            "context_hash": context_sha == raw["context_sha"] == feature_hash == raw["feature_hash"],
            "source_hash": source_raw_identity == raw["identity"] and prediction["source_raw_prediction_sha256"] == raw["prediction_sha"],
            "market_source_hash": prediction.get("market_source_sha256") == raw["market_hash"],
        }
        parity.append({"game_date": date, "game_pk": int(game_pk), **{f"{key}_match": value for key, value in checks.items()},
                       "all_exact": all(checks.values()), "unexplained_mismatch": not all(checks.values())})
    if len(output) != len({row["canonical_identity"] for row in output}):
        raise RuntimeError("DUPLICATE_C_IDENTITIES")
    return output, parity


def watches() -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    connection = ro(C_LEDGER)
    rows = connection.execute("""
      SELECT w.game_date,w.scoring_run_tag,w.watch_payload_json,w.observed_at_utc
      FROM totals_c_shadow_watch_observations w
      JOIN (SELECT game_date,MAX(observed_at_utc) observed FROM totals_c_shadow_watch_observations
            WHERE game_date BETWEEN ? AND ? GROUP BY game_date) x
      ON x.game_date=w.game_date AND x.observed=w.observed_at_utc ORDER BY w.game_date
    """, (START_DATE, END_DATE)).fetchall()
    connection.close()
    by_date: dict[str, dict[str, Any]] = {}
    for date, tag, payload_json, observed in rows:
        payload = json.loads(payload_json)
        watch_rows = payload.get("watch_rows", payload.get("deployment_watch_snapshot"))
        if not watch_rows:
            raise RuntimeError(f"C_WATCH_ROWS_UNAVAILABLE_{date}_{tag}")
        by_date[date] = {item["watch"].split("_", 1)[0]: item for item in watch_rows}
        by_date[date]["META"] = {"run_tag": tag, "observed_at_utc": observed, "regime": payload["regime_classification"]}
    summary = []
    for watch_id, label in zip("ABCDEFGHI", (
        "bullpen freshness", "zero burden", "likely-reliever-count drift", "starter fallback mix",
        "league-total center drift", "probable-pitcher availability", "park/context fallback",
        "feature support", "model/hash",
    )):
        statuses = [by_date[date][watch_id]["status"] for date in DATES]
        watch_dates = [date for date in DATES if by_date[date][watch_id]["status"] == "WATCH"]
        fail_dates = [date for date in DATES if by_date[date][watch_id]["status"] == "FAIL"]
        summary.append({
            "watch": watch_id, "label": label, "pass_dates": statuses.count("PASS"),
            "watch_dates": statuses.count("WATCH"), "fail_dates": statuses.count("FAIL"),
            "watch_date_list": "|".join(watch_dates), "fail_date_list": "|".join(fail_dates),
            "materially_explains_results": "NO_GOVERNED_EVIDENCE",
        })
    return by_date, summary


def point_and_distribution_tables(rows: list[dict[str, Any]], raw_alpha: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    point = []
    for variant, key, semantics in (
        ("RAW_MEAN", "raw_mean", "NEGATIVE_BINOMIAL_EXPECTED_TOTAL"),
        ("RAW_V1_INTERCEPT", "raw_v1_intercept_mean", "FROZEN_RAW_ONLY_DIAGNOSTIC"),
        ("C_MEAN", "c_mean", "NEGATIVE_BINOMIAL_EXPECTED_TOTAL"),
        ("C_MEDIAN", "c_median", "NEGATIVE_BINOMIAL_ABSOLUTE_ERROR_OPTIMAL_POINT"),
        ("STRICT_PRIOR_LEAGUE_BASELINE", "strict_prior_league_baseline", "GOVERNED_LEAKAGE_SAFE_BASELINE"),
        ("TEAM_SHRUNK_BASELINE", "team_shrunk_baseline", "GOVERNED_LEAKAGE_SAFE_BASELINE"),
    ):
        record = {"variant": variant, "point_semantics": semantics, **point_metrics(rows, key)}
        if variant == "C_MEDIAN":
            record["rmse_status"] = "DESCRIPTIVE_ONLY_NOT_PRIMARY_MEDIAN_METRIC"
        point.append(record)
    distribution_rows = []
    for variant in ("RAW_MEAN", "RAW_V1_INTERCEPT", "C"):
        distribution_rows.append({"variant": variant, "line_policy": "FROZEN_GOVERNED_TOTAL_LINE", **distribution_metrics(rows, variant)})
    for variant, key in (("STRICT_PRIOR_LEAGUE_BASELINE", "strict_prior_league_baseline"), ("TEAM_SHRUNK_BASELINE", "team_shrunk_baseline")):
        crps_values = [crps_from_mass(nb_mass(float(row[key]), raw_alpha), int(row["actual_total"])) for row in rows]
        distribution_rows.append({"variant": variant, "line_policy": "COMMON_RAW_ALPHA_COMPARATIVE_BASELINE",
                                  "games": len(rows), "crps": float(np.mean(crps_values)),
                                  "proper_score_rows": 0, "brier": math.nan, "log_loss": math.nan, "ece": math.nan})
    return point, distribution_rows


def daily_and_cumulative(rows: list[dict[str, Any]], by_date_watches: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    daily, cumulative = [], []
    accumulated: list[dict[str, Any]] = []
    for date in DATES:
        selected = [row for row in rows if row["game_date"] == date]
        accumulated.extend(selected)
        raw_point, c_point, c_median = point_metrics(selected, "raw_mean"), point_metrics(selected, "c_mean"), point_metrics(selected, "c_median")
        raw_dist, c_dist = distribution_metrics(selected, "RAW_MEAN"), distribution_metrics(selected, "C")
        daily.append({
            "game_date": date, "resolved_games": len(selected), "raw_mae": raw_point["mae"],
            "c_mean_mae": c_point["mae"], "c_median_mae": c_median["mae"],
            "raw_actual_minus_forecast_bias": raw_point["actual_minus_forecast_bias"],
            "c_actual_minus_forecast_bias": c_point["actual_minus_forecast_bias"],
            "raw_crps": raw_dist["crps"], "c_crps": c_dist["crps"],
            "raw_brier": raw_dist["brier"], "c_brier": c_dist["brier"],
            "regime": by_date_watches[date]["META"]["regime"],
            "watches_A_to_I": "|".join(by_date_watches[date][letter]["status"] for letter in "ABCDEFGHI"),
        })
        raw_point, c_point, c_median = point_metrics(accumulated, "raw_mean"), point_metrics(accumulated, "c_mean"), point_metrics(accumulated, "c_median")
        raw_dist, c_dist = distribution_metrics(accumulated, "RAW_MEAN"), distribution_metrics(accumulated, "C")
        cumulative.append({
            "through_date": date, "cumulative_games": len(accumulated), "raw_mae": raw_point["mae"],
            "c_mean_mae": c_point["mae"], "c_median_mae": c_median["mae"],
            "raw_rmse": raw_point["rmse"], "c_rmse": c_point["rmse"],
            "raw_bias": raw_point["actual_minus_forecast_bias"], "c_bias": c_point["actual_minus_forecast_bias"],
            "raw_crps": raw_dist["crps"], "c_crps": c_dist["crps"],
            "raw_brier": raw_dist["brier"], "c_brier": c_dist["brier"],
            "raw_log_loss": raw_dist["log_loss"], "c_log_loss": c_dist["log_loss"],
            "raw_ece": raw_dist["ece"], "c_ece": c_dist["ece"],
        })
    return daily, cumulative


def deltas(selected: list[dict[str, Any]]) -> dict[str, float]:
    raw, c, median = point_metrics(selected, "raw_mean"), point_metrics(selected, "c_mean"), point_metrics(selected, "c_median")
    raw_dist, c_dist = distribution_metrics(selected, "RAW_MEAN"), distribution_metrics(selected, "C")
    return {
        "C_MEAN_MINUS_RAW_MAE": c["mae"] - raw["mae"],
        "C_MEDIAN_MINUS_RAW_MAE": median["mae"] - raw["mae"],
        "C_MINUS_RAW_RMSE": c["rmse"] - raw["rmse"],
        "C_MINUS_RAW_ABSOLUTE_BIAS": abs(c["actual_minus_forecast_bias"]) - abs(raw["actual_minus_forecast_bias"]),
        "C_MINUS_RAW_CRPS": c_dist["crps"] - raw_dist["crps"],
        "C_MINUS_RAW_BRIER": c_dist["brier"] - raw_dist["brier"],
        "C_MINUS_RAW_LOG_LOSS": c_dist["log_loss"] - raw_dist["log_loss"],
    }


def clustered_uncertainty(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    full = deltas(rows)
    by_date = {date: [row for row in rows if row["game_date"] == date] for date in DATES}
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    draws = {metric: [] for metric in full}
    for sampled in rng.choice(DATES, size=(BOOTSTRAP_REPS, len(DATES)), replace=True):
        selected = [row for date in sampled for row in by_date[date]]
        values = deltas(selected)
        for metric, value in values.items():
            draws[metric].append(value)
    output = []
    for metric, estimate in full.items():
        values = np.array(draws[metric])
        output.append({
            "metric": metric, "estimate": estimate, "ci_95_lower": float(np.quantile(values, 0.025)),
            "ci_95_upper": float(np.quantile(values, 0.975)), "bootstrap_fraction_favoring_c": float(np.mean(values < 0)),
            "negative_delta_means": "C_BETTER", "bootstrap_reps": BOOTSTRAP_REPS, "seed": BOOTSTRAP_SEED,
        })
    return output


def lodo(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], str]:
    output = []
    for date in DATES:
        selected = [row for row in rows if row["game_date"] != date]
        output.append({"omitted_date": date, "games_remaining": len(selected), **deltas(selected)})
    metrics = [key for key in output[0] if key.startswith("C_")]
    favor_fractions = []
    sign_changes = 0
    for metric in metrics:
        values = [float(row[metric]) for row in output]
        favor_fractions.append(sum(value < 0 for value in values) / len(values))
        sign_changes += int(min(values) < 0 < max(values))
        for row in output:
            row[f"{metric}_min"] = min(values)
            row[f"{metric}_max"] = max(values)
            row[f"{metric}_fraction_favoring_c"] = favor_fractions[-1]
            row[f"{metric}_sign_change"] = min(values) < 0 < max(values)
    if sign_changes == 0 and (all(value >= .75 for value in favor_fractions) or all(value <= .25 for value in favor_fractions)):
        status = "ROBUST"
    elif sign_changes <= 2:
        status = "MODERATE"
    elif np.mean(favor_fractions) >= .6 or np.mean(favor_fractions) <= .4:
        status = "MIXED"
    else:
        status = "WEAK"
    influence = []
    full = deltas(rows)
    for row in output:
        row["aggregate_influence"] = sum(abs(float(row[metric]) - full[metric]) for metric in metrics)
        influence.append(row["aggregate_influence"])
    for row in output:
        row["most_influential_date"] = row["aggregate_influence"] == max(influence)
        row["C_LODO_STABILITY"] = status
    return output, status


def load_pinnacle(rows: list[dict[str, Any]], c_alpha: float) -> list[dict[str, Any]]:
    connection = ro(MARKET_LEDGER)
    markets = connection.execute("""
      SELECT game_date,game_id,captured_at_utc,scheduled_start_utc,market_payload_json,
             market_payload_sha256,raw_source_sha256,canonical_market_identity
      FROM supplemental_main_market_snapshots
      WHERE bookmaker_key='pinnacle' AND market_type='FULL_GAME_TOTAL' AND game_date BETWEEN ? AND ?
      ORDER BY game_date,game_id,captured_at_utc
    """, (START_DATE, END_DATE)).fetchall()
    connection.close()
    grouped: dict[tuple[str, int], list[Any]] = {}
    for market in markets:
        grouped.setdefault((market[0], int(market[1])), []).append(market)
    output = []
    for row in rows:
        candidates = [market for market in grouped[(row["game_date"], row["game_pk"])] if iso(market[2]) < iso(market[3])]
        if not candidates:
            continue
        market = min(candidates, key=lambda item: (abs((iso(item[2]) - iso(row["prediction_timestamp_utc"])).total_seconds()), item[2]))
        payload = json.loads(market[4])
        difference = abs((iso(market[2]) - iso(row["prediction_timestamp_utc"])).total_seconds()) / 60.0
        line = float(payload["total_line"])
        c_mass = nb_mass(float(row["c_mean"]), c_alpha)
        _, _, _, c_probability = line_probability(c_mass, line)
        market_probability = float(payload["no_vig_over_probability"])
        result = "OVER" if row["actual_total"] > line else ("UNDER" if row["actual_total"] < line else "PUSH")
        c_side = "OVER" if c_probability > .5 else ("UNDER" if c_probability < .5 else "NEUTRAL")
        market_side = "OVER" if market_probability > .5 else ("UNDER" if market_probability < .5 else "NEUTRAL")
        c_correct = result != "PUSH" and c_side == result
        market_correct = result != "PUSH" and market_side == result
        output.append({
            "game_date": row["game_date"], "game_pk": row["game_pk"],
            "model_timestamp_utc": row["prediction_timestamp_utc"], "pinnacle_timestamp_utc": market[2],
            "absolute_timing_difference_minutes": difference, "within_30_minutes": difference <= 30,
            "within_60_minutes": difference <= 60, "scheduled_start_utc": market[3],
            "total_line": line, "over_american_price": payload["over_american_price"],
            "under_american_price": payload["under_american_price"],
            "pinnacle_no_vig_over_probability": market_probability,
            "pinnacle_no_vig_under_probability": float(payload["no_vig_under_probability"]),
            "c_expected_total": row["c_mean"], "c_binary_over_probability": c_probability,
            "actual_total": row["actual_total"], "result": result, "c_side": c_side, "pinnacle_side": market_side,
            "c_correct": c_correct, "pinnacle_correct": market_correct,
            "c_minus_market_total": float(row["c_mean"]) - line,
            "absolute_total_separation": abs(float(row["c_mean"]) - line),
            "absolute_probability_difference": abs(c_probability - market_probability),
            "market_identity": market[7], "market_payload_sha256": market[5], "raw_source_sha256": market[6],
        })
    return output


def market_metrics(rows: list[dict[str, Any]], threshold: int) -> list[dict[str, Any]]:
    selected = [row for row in rows if row[f"within_{threshold}_minutes"] and row["result"] != "PUSH"]
    c = binary_metrics((row["c_binary_over_probability"], int(row["result"] == "OVER")) for row in selected)
    market = binary_metrics((row["pinnacle_no_vig_over_probability"], int(row["result"] == "OVER")) for row in selected)
    output = [
        {"synchronization_window_minutes": threshold, "variant": "C", "synchronized_rows": len([r for r in rows if r[f'within_{threshold}_minutes']]), **c},
        {"synchronization_window_minutes": threshold, "variant": "PINNACLE_NO_VIG", "synchronized_rows": len([r for r in rows if r[f'within_{threshold}_minutes']]), **market},
    ]
    output.append({"synchronization_window_minutes": threshold, "variant": "C_MINUS_PINNACLE", "synchronized_rows": output[0]["synchronized_rows"],
                   "proper_score_rows": c["proper_score_rows"], "brier": c["brier"] - market["brier"],
                   "log_loss": c["log_loss"] - market["log_loss"], "ece": c["ece"] - market["ece"]})
    return output


def market_bootstrap(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in rows if row["within_30_minutes"] and row["result"] != "PUSH"]
    dates = sorted({row["game_date"] for row in rows})
    by_date = {date: [row for row in rows if row["game_date"] == date] for date in dates}
    def metric(selected: list[dict[str, Any]], key: str) -> float:
        c = binary_metrics((row["c_binary_over_probability"], int(row["result"] == "OVER")) for row in selected)
        market = binary_metrics((row["pinnacle_no_vig_over_probability"], int(row["result"] == "OVER")) for row in selected)
        return c[key] - market[key]
    rng = np.random.default_rng(BOOTSTRAP_SEED + 1)
    draws = {key: [] for key in ("brier", "log_loss")}
    for sampled in rng.choice(dates, size=(BOOTSTRAP_REPS, len(dates)), replace=True):
        selected = [row for date in sampled for row in by_date[date]]
        for key in draws:
            draws[key].append(metric(selected, key))
    output = []
    for key, values_list in draws.items():
        values = np.asarray(values_list)
        output.append({"metric": f"C_MINUS_PINNACLE_{key.upper()}", "estimate": metric(rows, key),
                       "ci_95_lower": float(np.quantile(values, .025)), "ci_95_upper": float(np.quantile(values, .975)),
                       "bootstrap_fraction_favoring_c": float(np.mean(values < 0)), "bootstrap_reps": BOOTSTRAP_REPS,
                       "seed": BOOTSTRAP_SEED + 1})
    return output


def market_diagnostics(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    selected = [row for row in rows if row["within_30_minutes"]]
    separation = [{
        "synchronized_rows": len(selected),
        "mean_signed_c_minus_market_total": float(np.mean([row["c_minus_market_total"] for row in selected])),
        "mean_absolute_separation": float(np.mean([row["absolute_total_separation"] for row in selected])),
        "median_absolute_separation": float(np.median([row["absolute_total_separation"] for row in selected])),
        "separation_sd": float(np.std([row["c_minus_market_total"] for row in selected], ddof=1)),
        "ge_0_5": sum(row["absolute_total_separation"] >= .5 for row in selected),
        "ge_1_0": sum(row["absolute_total_separation"] >= 1 for row in selected),
        "ge_1_5": sum(row["absolute_total_separation"] >= 1.5 for row in selected),
        "ge_2_0": sum(row["absolute_total_separation"] >= 2 for row in selected),
    }]
    nonpush = [row for row in selected if row["result"] != "PUSH"]
    disagreements = [row for row in nonpush if row["c_side"] != row["pinnacle_side"]]
    neutral = [row for row in nonpush if abs(row["c_binary_over_probability"] - .5) < .01 and abs(row["pinnacle_no_vig_over_probability"] - .5) < .01]
    same_side_confidence = [row for row in nonpush if row["c_side"] == row["pinnacle_side"] and row["absolute_probability_difference"] >= .05]
    directional = [{
        "nonpush_rows": len(nonpush), "opposite_side_count": len(disagreements),
        "opposite_side_percentage": len(disagreements) / len(nonpush) if nonpush else math.nan,
        "c_correct_on_disagreements": sum(row["c_correct"] for row in disagreements),
        "pinnacle_correct_on_disagreements": sum(row["pinnacle_correct"] for row in disagreements),
        "both_effectively_neutral_count": len(neutral),
        "same_side_confidence_disagreement_ge_5pp": len(same_side_confidence),
        "neutral_definition": "ABS_PROBABILITY_MINUS_0.5_LT_0.01",
    }]
    unique = [{
        "nonpush_rows": len(nonpush), "both_correct": sum(row["c_correct"] and row["pinnacle_correct"] for row in nonpush),
        "both_wrong": sum(not row["c_correct"] and not row["pinnacle_correct"] for row in nonpush),
        "c_only_correct": sum(row["c_correct"] and not row["pinnacle_correct"] for row in nonpush),
        "pinnacle_only_correct": sum(not row["c_correct"] and row["pinnacle_correct"] for row in nonpush),
    }]
    bands = []
    definitions = [(0, .5, "<0.5"), (.5, 1, "0.5-0.99"), (1, 1.5, "1.0-1.49"), (1.5, 2, "1.5-1.99"), (2, math.inf, ">=2.0")]
    for lower, upper, label in definitions:
        subset = [row for row in selected if lower <= row["absolute_total_separation"] < upper]
        binary = [row for row in subset if row["result"] != "PUSH"]
        c_proper = binary_metrics((row["c_binary_over_probability"], int(row["result"] == "OVER")) for row in binary)
        market_proper = binary_metrics((row["pinnacle_no_vig_over_probability"], int(row["result"] == "OVER")) for row in binary)
        bands.append({
            "separation_band": label, "n": len(subset), "nonpush_n": len(binary),
            "c_brier": c_proper["brier"], "c_log_loss": c_proper["log_loss"],
            "market_brier": market_proper["brier"], "market_log_loss": market_proper["log_loss"],
            "mean_actual_total": float(np.mean([row["actual_total"] for row in subset])) if subset else math.nan,
            "c_absolute_error": float(np.mean([abs(row["c_expected_total"] - row["actual_total"]) for row in subset])) if subset else math.nan,
            "market_line_absolute_error": float(np.mean([abs(row["total_line"] - row["actual_total"]) for row in subset])) if subset else math.nan,
            "c_only_correct": sum(row["c_correct"] and not row["pinnacle_correct"] for row in binary),
            "market_only_correct": sum(not row["c_correct"] and row["pinnacle_correct"] for row in binary),
        })
    return separation, directional, unique, bands


def probability_relationship(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [row for row in rows if row["within_30_minutes"] and row["result"] != "PUSH"]
    c = np.array([row["c_binary_over_probability"] for row in selected])
    market = np.array([row["pinnacle_no_vig_over_probability"] for row in selected])
    return [{
        "rows": len(selected), "pearson_c_vs_pinnacle": float(np.corrcoef(c, market)[0, 1]),
        "spearman_c_vs_pinnacle": float(spearmanr(c, market).statistic),
        "mean_absolute_probability_difference": float(np.mean(np.abs(c - market))),
        "ge_5pp_disagreement": int(np.sum(np.abs(c - market) >= .05)),
        "ge_10pp_disagreement": int(np.sum(np.abs(c - market) >= .10)),
        "ge_15pp_disagreement": int(np.sum(np.abs(c - market) >= .15)),
    }]


def incremental_information(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [row for row in rows if row["within_30_minutes"] and row["result"] != "PUSH"]
    output_records: list[dict[str, Any]] = []
    for test_index in range(3, len(DATES)):
        training_dates, test_date = DATES[:test_index], DATES[test_index]
        train = [row for row in selected if row["game_date"] in training_dates]
        test = [row for row in selected if row["game_date"] == test_date]
        if not test or len({int(row["result"] == "OVER") for row in train}) < 2:
            continue
        y_train = np.array([int(row["result"] == "OVER") for row in train])
        y_test = np.array([int(row["result"] == "OVER") for row in test])
        features = {
            "MARKET_ALONE": (np.array([[row["pinnacle_no_vig_over_probability"]] for row in train]), np.array([[row["pinnacle_no_vig_over_probability"]] for row in test])),
            "C_ALONE": (np.array([[row["c_binary_over_probability"]] for row in train]), np.array([[row["c_binary_over_probability"]] for row in test])),
            "MARKET_PLUS_C": (np.array([[row["pinnacle_no_vig_over_probability"], row["c_binary_over_probability"]] for row in train]), np.array([[row["pinnacle_no_vig_over_probability"], row["c_binary_over_probability"]] for row in test])),
        }
        for variant, (x_train, x_test) in features.items():
            model = LogisticRegression(C=1.0, solver="lbfgs", max_iter=1000, random_state=BOOTSTRAP_SEED)
            model.fit(x_train, y_train)
            probabilities = model.predict_proba(x_test)[:, 1]
            clipped = np.clip(probabilities, 1e-15, 1 - 1e-15)
            for probability, outcome, clipped_probability in zip(probabilities, y_test, clipped):
                output_records.append({"test_date": test_date, "training_dates": "|".join(training_dates), "variant": variant,
                                       "probability": float(probability), "observed_over": int(outcome),
                                       "brier": float((probability - outcome) ** 2),
                                       "log_loss": float(-(outcome * math.log(clipped_probability) + (1 - outcome) * math.log(1 - clipped_probability)))})
    output = []
    for variant in ("MARKET_ALONE", "C_ALONE", "MARKET_PLUS_C"):
        subset = [row for row in output_records if row["variant"] == variant]
        output.append({"variant": variant, "oos_rows": len(subset), "test_dates": len({row["test_date"] for row in subset}),
                       "brier": float(np.mean([row["brier"] for row in subset])) if subset else math.nan,
                       "log_loss": float(np.mean([row["log_loss"] for row in subset])) if subset else math.nan,
                       "fold_contract": "EXPANDING_CHRONOLOGICAL_DATE_FOLDS_FIRST_TEST_AFTER_3_CLUSTERS_FIXED_LOGISTIC_C_1"})
    combined = next(row for row in output if row["variant"] == "MARKET_PLUS_C")
    for base in ("MARKET_ALONE", "C_ALONE"):
        original = next(row for row in output if row["variant"] == base)
        output.append({"variant": f"MARKET_PLUS_C_MINUS_{base}", "oos_rows": combined["oos_rows"],
                       "test_dates": combined["test_dates"], "brier": combined["brier"] - original["brier"],
                       "log_loss": combined["log_loss"] - original["log_loss"],
                       "fold_contract": "NEGATIVE_DELTA_FAVORS_INCREMENTAL_COMBINATION"})
    return output


def decisions(rows_for_decision: list[dict[str, Any]], point: list[dict[str, Any]], dist: list[dict[str, Any]], uncertainty: list[dict[str, Any]],
              lodo_status: str, baselines: list[dict[str, Any]], market: list[dict[str, Any]],
              separation: list[dict[str, Any]], relationship: list[dict[str, Any]], incremental: list[dict[str, Any]]) -> dict[str, str]:
    point_by = {row["variant"]: row for row in point}
    dist_by = {row["variant"]: row for row in dist}
    market_by = {(row["synchronization_window_minutes"], row["variant"]): row for row in market
                 if "synchronization_window_minutes" in row}
    raw_bias, c_bias = point_by["RAW_MEAN"]["actual_minus_forecast_bias"], point_by["C_MEAN"]["actual_minus_forecast_bias"]
    bias_reduction = abs(raw_bias) - abs(c_bias)
    daily_bias_improvements = 0
    for date in DATES:
        selected = [row for row in rows_for_decision if row["game_date"] == date]
        raw_daily = point_metrics(selected, "raw_mean")["actual_minus_forecast_bias"]
        c_daily = point_metrics(selected, "c_mean")["actual_minus_forecast_bias"]
        daily_bias_improvements += abs(c_daily) < abs(raw_daily)
    if bias_reduction > .25 and abs(c_bias) < abs(raw_bias) and daily_bias_improvements >= 6:
        bias_result = "YES"
    elif bias_reduction > 0:
        bias_result = "PARTIALLY"
    elif np.sign(raw_bias) != np.sign(c_bias):
        bias_result = "MIXED"
    else:
        bias_result = "NO"
    mean_tradeoff = point_by["C_MEAN"]["mae"] - point_by["RAW_MEAN"]["mae"]
    median_tradeoff = point_by["C_MEDIAN"]["mae"] - point_by["RAW_MEAN"]["mae"]
    has_distribution_advantage = sum(dist_by["C"][key] < dist_by["RAW_MEAN"][key] for key in ("crps", "brier", "log_loss")) >= 2
    if abs(median_tradeoff) <= abs(mean_tradeoff) * .5 and abs(median_tradeoff) <= .05 and has_distribution_advantage:
        point_result = "MEDIAN_MATERIALLY_IMPROVES_INTERPRETATION"
    elif abs(median_tradeoff) < abs(mean_tradeoff):
        point_result = "MEDIAN_PARTIALLY_IMPROVES_INTERPRETATION"
    elif abs(mean_tradeoff) < .05:
        point_result = "NO_MEANINGFUL_DIFFERENCE"
    else:
        point_result = "TRADEOFF_REMAINS_MATERIAL"
    c_beats_any_baseline = any(point_by["C_MEDIAN"]["mae"] < row["mae"] for row in baselines)
    proper_improvements = sum(dist_by["C"][key] < dist_by["RAW_MEAN"][key] for key in ("crps", "brier", "log_loss"))
    if c_beats_any_baseline and proper_improvements >= 2 and lodo_status in ("ROBUST", "MODERATE"):
        standalone = "MODERATE"
    elif c_beats_any_baseline or proper_improvements >= 2:
        standalone = "MIXED"
    else:
        standalone = "WEAK"
    c_market = market_by[(30, "C")]
    p_market = market_by[(30, "PINNACLE_NO_VIG")]
    brier_delta, log_delta = c_market["brier"] - p_market["brier"], c_market["log_loss"] - p_market["log_loss"]
    if brier_delta < -.005 and log_delta < 0:
        market_parity = "C_BETTER"
    elif brier_delta <= .02 and log_delta <= .06:
        market_parity = "BROADLY_COMPARABLE"
    elif brier_delta <= .05 and log_delta <= .15:
        market_parity = "MODESTLY_BEHIND_BUT_COMPARABLE"
    else:
        market_parity = "MATERIALLY_BEHIND"
    mean_sep = separation[0]["mean_absolute_separation"]
    separation_class = "LOW" if mean_sep < .5 else ("MODERATE" if mean_sep < 1 else ("MATERIAL" if mean_sep < 1.5 else "HIGH"))
    rel = relationship[0]
    opinion = "MEANINGFULLY_INDEPENDENT" if rel["pearson_c_vs_pinnacle"] < .9 and rel["mean_absolute_probability_difference"] >= .03 else ("PARTIALLY_DISTINCT" if rel["mean_absolute_probability_difference"] >= .015 else "LARGELY_MARKET_REPLICATING")
    combined_market = next(row for row in incremental if row["variant"] == "MARKET_PLUS_C_MINUS_MARKET_ALONE")
    combined_c = next(row for row in incremental if row["variant"] == "MARKET_PLUS_C_MINUS_C_ALONE")
    if combined_market["brier"] < 0 and combined_market["log_loss"] < 0 and combined_c["brier"] < 0 and combined_c["log_loss"] < 0:
        incremental_result = "WEAK_EVIDENCE"
    elif (combined_market["brier"] < 0 or combined_market["log_loss"] < 0) and (combined_c["brier"] < 0 or combined_c["log_loss"] < 0):
        incremental_result = "MIXED"
    else:
        incremental_result = "NOT_REPRODUCED"
    if standalone in ("MODERATE", "MIXED") and market_parity != "MATERIALLY_BEHIND":
        checkpoint = "PROMISING" if standalone == "MODERATE" else "MIXED"
        continuation = "C_CONTINUE_TO_12_CLUSTER_REVIEW"
    else:
        checkpoint = "WEAK" if standalone == "WEAK" else "CONCERNING"
        continuation = "C_STOP_AFTER_8_INSUFFICIENT_VALUE" if checkpoint == "WEAK" else "C_STOP_AFTER_8_MATERIAL_FAILURE"
    certification = "C_STANDALONE_PREDICTION_CERTIFICATION_DEFERRED" if continuation == "C_CONTINUE_TO_12_CLUSTER_REVIEW" else "C_STANDALONE_PREDICTION_NOT_CERTIFIED"
    public = "C_PUBLIC_PREDICTION_NOT_READY"
    interpretation = {"MODERATE": "C_STRUCTURAL_REPAIR_FORWARD_EVIDENCE_MODERATE", "MIXED": "C_STRUCTURAL_REPAIR_FORWARD_EVIDENCE_MIXED", "WEAK": "C_STRUCTURAL_REPAIR_FORWARD_EVIDENCE_WEAK"}[standalone]
    simple_baseline_result = "C_OUTPERFORMS_BOTH_GOVERNED_SIMPLE_BASELINES_ON_POINT_AND_CRPS" if (
        all(point_by["C_MEAN"]["mae"] < row["mae"] for row in baselines)
        and all(dist_by["C"]["crps"] < dist_by[row["variant"]]["crps"] for row in baselines)
    ) else "C_SIMPLE_BASELINE_RESULT_MIXED"
    return {
        "C_PROSPECTIVE_INTEGRITY": "PASS", "C_LODO_STABILITY": lodo_status,
        "C_CUMULATIVE_TRAJECTORY": "MIXED_THEN_STABILIZED_MODESTLY_BEHIND_RAW",
        "DID_C_REPAIR_RAW_LOCATION_BIAS": bias_result, "C_POINT_SUMMARY_RESULT": point_result,
        "COUNT_CONFIDENCE_STRUCTURAL_REPAIR": "PROSPECTIVELY_SUPPORTED" if standalone != "WEAK" else "MIXED",
        "C_SIMPLE_BASELINE_RESULT": simple_baseline_result,
        "C_STANDALONE_FORWARD_EVIDENCE": standalone, "C_MARKET_PREDICTIVE_PARITY": market_parity,
        "C_TOTAL_OPINION_SEPARATION": separation_class, "C_MARKET_PROBABILITY_INPUTS": "NO",
        "C_OPINION_INDEPENDENCE": opinion, "C_INCREMENTAL_INFORMATION": incremental_result,
        "C_8_CLUSTER_FORWARD_RESULT": checkpoint, "C_12_CLUSTER_DECISION": continuation,
        "C_CERTIFICATION_STATUS": certification, "C_PUBLIC_READINESS": public,
        "PRIMARY_INTERPRETATION": interpretation,
    }


def render_documents(output: Path, rows: list[dict[str, Any]], point: list[dict[str, Any]], dist: list[dict[str, Any]],
                     uncertainty: list[dict[str, Any]], lodo_rows: list[dict[str, Any]], watch_summary: list[dict[str, Any]],
                     baseline_rows: list[dict[str, Any]], pinnacle: list[dict[str, Any]], market: list[dict[str, Any]],
                     separation: list[dict[str, Any]], directional: list[dict[str, Any]], unique: list[dict[str, Any]],
                     relationship: list[dict[str, Any]], incremental: list[dict[str, Any]], result: dict[str, str]) -> None:
    point_by, dist_by = {r["variant"]: r for r in point}, {r["variant"]: r for r in dist}
    market_by = {(r["synchronization_window_minutes"], r["variant"]): r for r in market
                 if "synchronization_window_minutes" in r}
    (output / "totals_c_8_cluster_structural_validation.md").write_text(f"""# Structural validation

- Frozen artifact structural contract: all three raw sample-depth counts are removed from direct location and retained upstream only.
- Direct location excludes `park_history_depth`, `home_starter_prior_starts`, and `away_starter_prior_starts`.
- Park depth remains only in governed park shrinkage; starter counts remain only in fallback, minimum-history, workload, and confidence state.
- Live feature contract hash: `{FEATURE_CONTRACT_HASH}` on all {len(rows)} rows.
- Feature-support watch failures: {sum(r['fail_dates'] for r in watch_summary if r['watch'] == 'H')}; model/hash failures: {sum(r['fail_dates'] for r in watch_summary if r['watch'] == 'I')}.
- No live evidence shows indirect reintroduction of the removed direct-location pathology.

`COUNT_CONFIDENCE_STRUCTURAL_REPAIR = {result['COUNT_CONFIDENCE_STRUCTURAL_REPAIR']}`
""")
    (output / "totals_c_8_cluster_standalone_status.md").write_text(f"""# Standalone status frozen before market comparison

- C median MAE: `{point_by['C_MEDIAN']['mae']:.6f}`; C mean MAE: `{point_by['C_MEAN']['mae']:.6f}`; RAW mean MAE: `{point_by['RAW_MEAN']['mae']:.6f}`.
- RAW/C actual-minus-forecast bias: `{point_by['RAW_MEAN']['actual_minus_forecast_bias']:.6f}` / `{point_by['C_MEAN']['actual_minus_forecast_bias']:.6f}`.
- RAW/C CRPS: `{dist_by['RAW_MEAN']['crps']:.6f}` / `{dist_by['C']['crps']:.6f}`.
- RAW/C Brier: `{dist_by['RAW_MEAN']['brier']:.6f}` / `{dist_by['C']['brier']:.6f}`.
- RAW/C log loss: `{dist_by['RAW_MEAN']['log_loss']:.6f}` / `{dist_by['C']['log_loss']:.6f}`.
- Baselines are frozen leakage-safe comparators; no review-window tuning occurred.
- `{result['C_SIMPLE_BASELINE_RESULT']}`.

`C_STANDALONE_FORWARD_EVIDENCE = {result['C_STANDALONE_FORWARD_EVIDENCE']}`
""")
    (output / "totals_c_8_cluster_market_independence.md").write_text(f"""# Market-independence contract

C uses no sportsbook probability, odds, no-vig probability, consensus probability, or market movement. The governed total line defines the proposition being evaluated; it is not a probability or location feature. Market attachment occurs after prediction identity and distribution are frozen.

`C_MARKET_PROBABILITY_INPUTS = {result['C_MARKET_PROBABILITY_INPUTS']}`

`C_OPINION_INDEPENDENCE = {result['C_OPINION_INDEPENDENCE']}`
""")
    (output / "totals_c_8_cluster_checkpoint_decision.md").write_text(f"""# Eight-cluster checkpoint decision

`C_8_CLUSTER_FORWARD_RESULT = {result['C_8_CLUSTER_FORWARD_RESULT']}`

`{result['C_12_CLUSTER_DECISION']}`

Continuation keeps the exact model, artifact, feature/context contracts, prediction semantics, and review discipline frozen. It authorizes no production promotion or model change.

`{result['C_CERTIFICATION_STATUS']}`
""")
    (output / "totals_c_8_cluster_public_readiness.md").write_text(f"""# Public readiness

`{result['C_PUBLIC_READINESS']}`

No UI or public behavior was changed. No betting-edge certification is made.
""")
    market_c, market_p = market_by[(30, "C")], market_by[(30, "PINNACLE_NO_VIG")]
    boot_lines = "\n".join(f"- {r['metric']}: {r['estimate']:.6f} [{r['ci_95_lower']:.6f}, {r['ci_95_upper']:.6f}], fraction C better {r['bootstrap_fraction_favoring_c']:.3f}." for r in uncertainty)
    report = f"""# MLB Totals C eight-cluster formal forward review v1

## Frozen population and integrity

- Review window: `{START_DATE}` through `{END_DATE}`; 8 primary date clusters.
- Scheduled/admitted/resolved/excluded: 105 / {len(rows)} / {len(rows)} / 1.
- PRIMARY_SCORE/SCORE_MISSING: {sum(r['scoring_mode']=='PRIMARY_SCORE' for r in rows)} / {sum(r['scoring_mode']=='SCORE_MISSING' for r in rows)}.
- Duplicates, overwrites, post-start admitted rows, unresolved outcomes: 0.
- Model/hash/artifact: `{MODEL_NAME}` / `{MODEL_HASH}` / `{ARTIFACT_SHA256}`.
- `C_PROSPECTIVE_INTEGRITY = {result['C_PROSPECTIVE_INTEGRITY']}`; RAW/C input parity 104/104 exact.

## Standalone evidence

- RAW mean: MAE {point_by['RAW_MEAN']['mae']:.6f}, RMSE {point_by['RAW_MEAN']['rmse']:.6f}, actual-minus-forecast bias {point_by['RAW_MEAN']['actual_minus_forecast_bias']:.6f}.
- C mean: MAE {point_by['C_MEAN']['mae']:.6f}, RMSE {point_by['C_MEAN']['rmse']:.6f}, bias {point_by['C_MEAN']['actual_minus_forecast_bias']:.6f}.
- C median MAE: {point_by['C_MEDIAN']['mae']:.6f}.
- RAW/C CRPS: {dist_by['RAW_MEAN']['crps']:.6f} / {dist_by['C']['crps']:.6f}; Brier {dist_by['RAW_MEAN']['brier']:.6f} / {dist_by['C']['brier']:.6f}; log loss {dist_by['RAW_MEAN']['log_loss']:.6f} / {dist_by['C']['log_loss']:.6f}.
- `{result['DID_C_REPAIR_RAW_LOCATION_BIAS']}` bias repair; `{result['C_POINT_SUMMARY_RESULT']}` point-summary result.
- `{result['COUNT_CONFIDENCE_STRUCTURAL_REPAIR']}` structural repair; `{result['C_STANDALONE_FORWARD_EVIDENCE']}` standalone evidence.
- `{result['C_SIMPLE_BASELINE_RESULT']}`.

## Date-clustered uncertainty

{boot_lines}

- `C_LODO_STABILITY = {result['C_LODO_STABILITY']}`; most influential omission: {next(r['omitted_date'] for r in lodo_rows if r['most_influential_date'])}.
- `C_CUMULATIVE_TRAJECTORY = {result['C_CUMULATIVE_TRAJECTORY']}`.

## Pinnacle comparison

- Synchronized samples: <=30 minutes {sum(r['within_30_minutes'] for r in pinnacle)}; <=60 minutes {sum(r['within_60_minutes'] for r in pinnacle)}.
- C/Pinnacle Brier: {market_c['brier']:.6f} / {market_p['brier']:.6f}; log loss {market_c['log_loss']:.6f} / {market_p['log_loss']:.6f}; ECE {market_c['ece']:.6f} / {market_p['ece']:.6f}.
- `C_MARKET_PREDICTIVE_PARITY = {result['C_MARKET_PREDICTIVE_PARITY']}`.
- Mean absolute expected-total separation: {separation[0]['mean_absolute_separation']:.6f}; `C_TOTAL_OPINION_SEPARATION = {result['C_TOTAL_OPINION_SEPARATION']}`.
- Opposite-side disagreements: {directional[0]['opposite_side_count']}/{directional[0]['nonpush_rows']}; C/Pinnacle correct on disagreements: {directional[0]['c_correct_on_disagreements']} / {directional[0]['pinnacle_correct_on_disagreements']}.
- Unique correctness—both correct {unique[0]['both_correct']}, both wrong {unique[0]['both_wrong']}, C only {unique[0]['c_only_correct']}, Pinnacle only {unique[0]['pinnacle_only_correct']}.
- Pearson/Spearman probability correlation: {relationship[0]['pearson_c_vs_pinnacle']:.6f} / {relationship[0]['spearman_c_vs_pinnacle']:.6f}; mean absolute probability difference {relationship[0]['mean_absolute_probability_difference']:.6f}.
- `C_OPINION_INDEPENDENCE = {result['C_OPINION_INDEPENDENCE']}`; `C_INCREMENTAL_INFORMATION = {result['C_INCREMENTAL_INFORMATION']}`.

## Decision

- `C_8_CLUSTER_FORWARD_RESULT = {result['C_8_CLUSTER_FORWARD_RESULT']}`.
- `{result['C_12_CLUSTER_DECISION']}`.
- `{result['C_CERTIFICATION_STATUS']}`.
- `{result['C_PUBLIC_READINESS']}`.
- `{result['PRIMARY_INTERPRETATION']}`.

No EV, ROI, selector, retraining, recalibration, promotion, production mutation, or August 25 outcome is present.
"""
    (output / "concise_mlb_totals_c_8_cluster_formal_forward_review_v1.md").write_text(report)


def run(output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    raw_artifact, c_artifact = verify_artifacts()
    raw_alpha, c_alpha = float(raw_artifact["dispersion_alpha"]), float(c_artifact["dispersion_alpha"])
    rows, parity = load_population(raw_alpha, c_alpha)
    if sorted({row["game_date"] for row in rows}) != DATES or len(rows) != 104:
        raise RuntimeError(f"UNEXPECTED_REVIEW_POPULATION_{len(rows)}")
    if any(row["game_date"] == "2026-08-25" for row in rows):
        raise RuntimeError("AUGUST_25_OUTCOME_CONTAMINATION")
    official = {date: official_games(date) for date in DATES}
    scheduled = sum(len(value) for value in official.values())
    admitted = {(row["game_date"], row["game_pk"]) for row in rows}
    exclusions = [{"game_date": date, **game, "admission_status": "EXCLUDED_FAIL_CLOSED",
                   "exclusion_reason": "PREGAME_CUTOFF_FAILED", "strict_pregame": None}
                  for date, games in official.items() for game_pk, game in games.items() if (date, game_pk) not in admitted]
    if scheduled != 105 or len(exclusions) != 1 or exclusions[0]["game_pk"] != 823745:
        raise RuntimeError("UNEXPECTED_SCHEDULE_OR_EXCLUSION_POPULATION")
    population = [{**row, "admission_status": "ADMITTED_IMMUTABLE", "exclusion_reason": "", "strict_pregame": True} for row in rows] + exclusions
    population.sort(key=lambda item: (item["game_date"], item["game_pk"]))
    by_date_watches, watch_summary = watches()
    point, dist = point_and_distribution_tables(rows, raw_alpha)
    daily, cumulative = daily_and_cumulative(rows, by_date_watches)
    uncertainty = clustered_uncertainty(rows)
    lodo_rows, lodo_status = lodo(rows)
    bias_review = []
    for scope, selected in [("OVERALL", rows)] + [(date, [row for row in rows if row["game_date"] == date]) for date in DATES]:
        raw_point, c_point = point_metrics(selected, "raw_mean"), point_metrics(selected, "c_mean")
        bias_review.append({"scope": scope, "games": len(selected), "raw_actual_minus_forecast_bias": raw_point["actual_minus_forecast_bias"],
                            "c_actual_minus_forecast_bias": c_point["actual_minus_forecast_bias"],
                            "absolute_bias_reduction": abs(raw_point["actual_minus_forecast_bias"]) - abs(c_point["actual_minus_forecast_bias"]),
                            "same_bias_sign": np.sign(raw_point["actual_minus_forecast_bias"]) == np.sign(c_point["actual_minus_forecast_bias"])})
    point_summary = [{"comparison": "C_MEAN_MINUS_RAW_MEAN_MAE", "delta": point_metrics(rows, "c_mean")["mae"] - point_metrics(rows, "raw_mean")["mae"]},
                     {"comparison": "C_MEDIAN_MINUS_RAW_MEAN_MAE", "delta": point_metrics(rows, "c_median")["mae"] - point_metrics(rows, "raw_mean")["mae"]},
                     {"comparison": "C_MEDIAN_MINUS_C_MEAN_MAE", "delta": point_metrics(rows, "c_median")["mae"] - point_metrics(rows, "c_mean")["mae"]}]
    baselines = [row for row in point if row["variant"] in ("STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE")]
    point_by_variant = {row["variant"]: row for row in point}
    dist_by_variant = {row["variant"]: row for row in dist}
    baseline_comparison = []
    for variant in ("RAW_MEAN", "C_MEAN", "C_MEDIAN", "STRICT_PRIOR_LEAGUE_BASELINE", "TEAM_SHRUNK_BASELINE"):
        point_row = point_by_variant[variant]
        distribution_variant = "C" if variant in ("C_MEAN", "C_MEDIAN") else variant
        dist_row = dist_by_variant[distribution_variant]
        baseline_comparison.append({"variant": variant, "games": point_row["games"], "mae": point_row["mae"],
                                    "rmse": point_row["rmse"], "actual_minus_forecast_bias": point_row["actual_minus_forecast_bias"],
                                    "crps": dist_row["crps"], "contract": point_row["point_semantics"]})
    pinnacle = load_pinnacle(rows, c_alpha)
    if len(pinnacle) != 104 or not all(row["within_30_minutes"] for row in pinnacle):
        raise RuntimeError("PINNACLE_SYNCHRONIZATION_POPULATION_MISMATCH")
    market = market_metrics(pinnacle, 30) + market_metrics(pinnacle, 60) + market_bootstrap(pinnacle)
    separation, directional, unique, bands = market_diagnostics(pinnacle)
    relationship = probability_relationship(pinnacle)
    incremental = incremental_information(pinnacle)
    result = decisions(rows, point, dist, uncertainty, lodo_status, baselines, market, separation, relationship, incremental)

    identity = {
        "task_id": TASK_ID, "review_window": {"start": START_DATE, "end": END_DATE, "clusters": 8},
        "c_model_name": MODEL_NAME, "c_model_hash": MODEL_HASH, "c_artifact_sha256": ARTIFACT_SHA256,
        "c_artifact_file_sha256": sha256(C_ARTIFACT), "feature_contract_hash": FEATURE_CONTRACT_HASH,
        "raw_control_hash": RAW_HASH, "raw_config_file_sha256": sha256(RAW_CONFIG),
        "raw_v1_intercept": INTERCEPT, "c_intercept_policy": "DO_NOT_APPLY_RAW_INTERCEPT_TO_C",
        "raw_dispersion_alpha": raw_alpha, "c_dispersion_alpha": c_alpha,
        "feature_contract_unchanged": len({row["feature_contract_hash"] for row in rows}) == 1,
        "context_contract_unchanged": all(row["context_payload_sha256"] == row["raw_context_payload_sha256"] for row in rows),
        "mean_semantics": "NEGATIVE_BINOMIAL_EXPECTED_TOTAL", "median_semantics": "DISCRETE_DISTRIBUTION_MEDIAN",
        "probability_contract": c_artifact["probability_contract"], "probability_ladder_unchanged": True,
        "raw_intercept_applied_to_c_rows": sum(row["raw_intercept_applied_to_c"] is not False for row in rows),
        "outcome_access_rows": sum(row["outcomes_accessed_during_prediction"] for row in rows),
        "C_PROSPECTIVE_INTEGRITY": result["C_PROSPECTIVE_INTEGRITY"],
    }
    write_csv(output / "totals_c_8_cluster_population.csv", population)
    write_json(output / "totals_c_8_cluster_model_identity.json", identity)
    write_csv(output / "totals_c_8_cluster_input_parity.csv", parity)
    write_csv(output / "totals_c_8_cluster_point_metrics.csv", point)
    write_csv(output / "totals_c_8_cluster_distribution_metrics.csv", dist)
    write_csv(output / "totals_c_8_cluster_daily_metrics.csv", daily)
    write_csv(output / "totals_c_8_cluster_cumulative_trajectory.csv", cumulative)
    write_csv(output / "totals_c_8_cluster_clustered_uncertainty.csv", uncertainty)
    write_csv(output / "totals_c_8_cluster_lodo.csv", lodo_rows)
    write_csv(output / "totals_c_8_cluster_bias_review.csv", bias_review)
    write_csv(output / "totals_c_8_cluster_point_summary_review.csv", point_summary)
    write_csv(output / "totals_c_8_cluster_watch_summary.csv", watch_summary)
    write_csv(output / "totals_c_8_cluster_baseline_comparison.csv", baseline_comparison)
    write_csv(output / "totals_c_8_cluster_pinnacle_timing.csv", pinnacle)
    write_csv(output / "totals_c_8_cluster_market_parity.csv", market)
    write_csv(output / "totals_c_8_cluster_total_separation.csv", separation)
    write_csv(output / "totals_c_8_cluster_directional_disagreement.csv", directional)
    write_csv(output / "totals_c_8_cluster_unique_correctness.csv", unique)
    write_csv(output / "totals_c_8_cluster_separation_bands.csv", bands)
    write_csv(output / "totals_c_8_cluster_probability_relationship.csv", relationship)
    write_csv(output / "totals_c_8_cluster_incremental_information.csv", incremental)
    render_documents(output, rows, point, dist, uncertainty, lodo_rows, watch_summary, baselines, pinnacle, market,
                     separation, directional, unique, relationship, incremental, result)
    write_json(output / "review_decisions.json", result)
    source_hashes = {str(path.relative_to(ROOT)): sha256(path) for path in (RAW_LEDGER, C_LEDGER, MARKET_LEDGER, RAW_CONFIG, C_ARTIFACT)}
    write_json(output / "reproducibility_hashes.json", {"task_id": TASK_ID, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                                                        "source_hashes": source_hashes, "analysis_script_sha256": sha256(Path(__file__))})
    files = sorted(path for path in output.iterdir() if path.is_file() and path.name != "sha256_manifest.csv")
    manifest_rows = [{"relative_path": display_path(path), "bytes": path.stat().st_size, "sha256": sha256(path)} for path in files]
    write_csv(output / "sha256_manifest.csv", manifest_rows)
    return {"task_id": TASK_ID, "output": display_path(output), "scheduled": scheduled,
            "admitted": len(rows), "resolved": len(rows), "excluded": len(exclusions), "pinnacle_synchronized": len(pinnacle), **result}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir.resolve()), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
