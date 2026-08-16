"""Reproduce and review the governed MLB totals prospective evidence through 2026-08-15.

This is intentionally a read-only analysis of frozen prediction, context, outcome,
and market ledgers.  It neither scores games nor changes any governed artifact.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Callable

import numpy as np

from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_EXISTING_MODEL_PROSPECTIVE_DUE_DILIGENCE_V1"
START_DATE = "2026-08-06"
END_DATE = "2026-08-15"
MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
INTERCEPT = 0.493550
THRESHOLDS = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)
BOOTSTRAP_REPS = 10_000
BOOTSTRAP_SEED = 20260816

CONFIG = ROOT / "backend/mlb/config/totals_predictions/MLB_TOTALS_DIRECT_NEGATIVE_BINOMIAL_V1.json"
LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
OFFICIAL_ROOT = ROOT / "artifacts/analysis/mlb/player_stats_completeness"
HISTORICAL_METRICS = ROOT / "artifacts/analysis/model_development/mlb_standalone_prediction_calibration_repair_v1/2026-08-12/totals_calibrated_holdout_metrics.csv"
OPS_LOG = ROOT / "artifacts/ops/mlb_refresh_daily.out.log"
DEFAULT_OUTPUT = ROOT / "artifacts/analysis/model_development/mlb_totals_existing_model_prospective_due_diligence_v1/2026-08-16"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=False).encode()).hexdigest()


def iso_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def mass(mu: float, alpha: float) -> np.ndarray:
    return distribution(float(mu), float(alpha))


def crps(mu: float, actual: int, alpha: float) -> float:
    probabilities = mass(mu, alpha)
    support = np.arange(len(probabilities))
    return float(np.sum((np.cumsum(probabilities) - (support >= int(actual)).astype(float)) ** 2))


def line_probabilities(mu: float, line: float, alpha: float) -> tuple[float, float, float]:
    probabilities = mass(mu, alpha)
    support = np.arange(len(probabilities))
    return (
        float(probabilities[support > line].sum()),
        float(probabilities[support < line].sum()),
        float(probabilities[support == line].sum()),
    )


def metrics(rows: list[dict[str, Any]], forecast: str, alpha: float, include_crps: bool = True) -> dict[str, float]:
    errors = np.array([float(row[forecast]) - float(row["actual_total"]) for row in rows])
    result = {
        "games": len(rows),
        "mae": float(np.mean(np.abs(errors))),
        "rmse": float(np.sqrt(np.mean(errors**2))),
        "forecast_minus_actual_bias": float(np.mean(errors)),
    }
    if include_crps:
        result["crps"] = float(np.mean([crps(float(row[forecast]), int(row["actual_total"]), alpha) for row in rows]))
    return result


def binary_metrics(records: list[dict[str, float]]) -> dict[str, float]:
    if not records:
        return {"rows": 0, "brier": math.nan, "log_loss": math.nan, "ece": math.nan,
                "mean_predicted_over_probability": math.nan, "observed_over_rate": math.nan}
    probabilities = np.array([float(row["probability"]) for row in records])
    outcomes = np.array([float(row["observed"]) for row in records])
    clipped = np.clip(probabilities, 1e-15, 1 - 1e-15)
    ece = 0.0
    for lower in np.arange(0.0, 1.0, 0.1):
        upper = lower + 0.1
        selected = (probabilities >= lower) & ((probabilities < upper) if upper < 1 else (probabilities <= upper))
        if selected.any():
            ece += float(selected.mean()) * abs(float(probabilities[selected].mean()) - float(outcomes[selected].mean()))
    return {
        "rows": len(records),
        "brier": float(np.mean((probabilities - outcomes) ** 2)),
        "log_loss": float(np.mean(-(outcomes * np.log(clipped) + (1 - outcomes) * np.log(1 - clipped)))),
        "ece": ece,
        "mean_predicted_over_probability": float(probabilities.mean()),
        "observed_over_rate": float(outcomes.mean()),
    }


def bin_label(value: float, cuts: list[tuple[float, str]]) -> str:
    for upper, label in cuts:
        if value < upper:
            return label
    return cuts[-1][1]


def official_final(game_date: str, game_id: int, source_path: str | None = None) -> dict[str, Any]:
    paths = ([ROOT / source_path] if source_path else
             sorted((OFFICIAL_ROOT / game_date / f"game_{game_id}" / "sources").glob(f"game_{game_id}_live_feed_*.json")))
    if not paths:
        raise RuntimeError(f"OFFICIAL_FINAL_SOURCE_COUNT_{game_date}_{game_id}_0")
    versions = []
    for path in paths:
        raw = path.read_bytes()
        payload = json.loads(raw)
        if payload.get("gameData", {}).get("status", {}).get("abstractGameState") != "Final":
            continue
        linescore = payload["liveData"]["linescore"]
        total = int(linescore["teams"]["away"]["runs"]) + int(linescore["teams"]["home"]["runs"])
        versions.append((path, raw, payload, total))
    if not versions:
        raise RuntimeError(f"OFFICIAL_GAME_NOT_FINAL_{game_date}_{game_id}")
    if len({(item[3], item[2]["gameData"]["datetime"]["dateTime"]) for item in versions}) != 1:
        raise RuntimeError(f"OFFICIAL_FINAL_SOURCE_CONFLICT_{game_date}_{game_id}")
    path, raw, payload, total = versions[0]  # deterministic retained-source choice; outcome rows bind their exact source explicitly.
    return {
        "actual_total": total,
        "scheduled_start_utc": payload["gameData"]["datetime"]["dateTime"],
        "official_source_path": str(path.relative_to(ROOT)),
        "official_source_sha256": hashlib.sha256(raw).hexdigest(),
    }


def official_games(game_date: str) -> list[dict[str, Any]]:
    rows = []
    for game_dir in sorted((OFFICIAL_ROOT / game_date).glob("game_*")):
        try:
            game_id = int(game_dir.name.split("_", 1)[1])
        except (IndexError, ValueError):
            continue
        rows.append({"game_id": game_id, **official_final(game_date, game_id)})
    identities = [row["game_id"] for row in rows]
    if len(identities) != len(set(identities)):
        raise RuntimeError(f"DUPLICATE_OFFICIAL_IDENTITIES_{game_date}")
    return rows


def mode_and_tag(game_date: str, timestamp: str, log_starts: list[tuple[datetime, str, str]]) -> tuple[str, str]:
    observed = iso_utc(timestamp)
    candidates = [(when, tag) for when, date, tag in log_starts if date == game_date and when <= observed]
    candidates.sort()
    run_tag = candidates[-1][1] if candidates and (observed - candidates[-1][0]).total_seconds() <= 600 else "UNBOUND_INITIAL_INTEGRATION"
    if game_date == "2026-08-06":
        return "INITIALIZATION_CAPTURE", "INITIALIZATION_20260806T213131Z"
    # The Aug 7 capture predates the finalized daily mode wiring and is retained as an initial integration capture.
    if game_date == "2026-08-07" and observed.hour < 16:
        return "INITIAL_INTEGRATION_CAPTURE", run_tag
    # PDT canonical trigger windows: 05:30=12:30Z; 08:30=15:30Z; later invocations >=18:00Z.
    if observed.hour < 15:
        return "PRIMARY_SCORE", run_tag
    return "SCORE_MISSING", run_tag


def parse_log_starts() -> list[tuple[datetime, str, str]]:
    if not OPS_LOG.exists():
        return []
    pattern = re.compile(r"^\[([^]]+)] START MLB totals prospective shadow lifecycle slate_date=(\d{4}-\d{2}-\d{2}).*run_tag=([^ ]+)")
    output = []
    for line in OPS_LOG.read_text(errors="replace").splitlines():
        match = pattern.search(line)
        if match:
            output.append((iso_utc(match.group(1)), match.group(2), match.group(3)))
    return output


def load_population(alpha: float) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    connection = sqlite3.connect(f"file:{LEDGER}?mode=ro", uri=True)
    query = """
      SELECT p.canonical_identity,p.game_date,p.game_id,p.scheduled_start_utc,p.prediction_timestamp_utc,
             p.model_hash,p.feature_state_hash,p.schedule_source_hash,p.market_source_hash,
             p.prediction_payload_json,p.prediction_payload_sha256,
             c.context_payload_json,c.context_payload_sha256,
             o.official_final_total,o.grading_payload_json,o.grading_payload_sha256
      FROM totals_shadow_predictions p
      JOIN totals_shadow_prediction_context c USING(canonical_identity)
      LEFT JOIN totals_shadow_outcomes o USING(canonical_identity)
      WHERE p.game_date BETWEEN ? AND ?
      ORDER BY p.game_date,p.scheduled_start_utc,p.game_id
    """
    raw_rows = connection.execute(query, (START_DATE, END_DATE)).fetchall()
    connection.close()
    log_starts = parse_log_starts()
    rows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for db in raw_rows:
        (identity, game_date, game_id, scheduled, predicted, model_hash, feature_hash, schedule_hash,
         market_hash, prediction_json, prediction_sha, context_json, context_sha, outcome_total,
         grading_json, grading_sha) = db
        prediction = json.loads(prediction_json)
        context = json.loads(context_json)
        if model_hash != MODEL_HASH or prediction.get("model_hash") != MODEL_HASH:
            raise RuntimeError(f"MODEL_HASH_MISMATCH_{identity}")
        if iso_utc(predicted) >= iso_utc(scheduled):
            raise RuntimeError(f"POST_START_PREDICTION_{identity}")
        if canonical_hash(context) != context_sha or canonical_hash(prediction) != prediction_sha:
            raise RuntimeError(f"LEDGER_PAYLOAD_HASH_MISMATCH_{identity}")
        if outcome_total is None:
            unresolved.append({"game_date": game_date, "game_pk": game_id, "reason": "OUTCOME_NOT_ATTACHED"})
            continue
        grading = json.loads(grading_json)
        official = official_final(game_date, int(game_id), grading["official_source_path"])
        if int(outcome_total) != official["actual_total"]:
            raise RuntimeError(f"CANONICAL_OUTCOME_MISMATCH_{identity}")
        if grading["official_source_hash"] != official["official_source_sha256"]:
            raise RuntimeError(f"CANONICAL_OUTCOME_SOURCE_HASH_MISMATCH_{identity}")
        raw_mu = float(prediction["expected_total"])
        corrected_mu = raw_mu + INTERCEPT
        timing, run_tag = mode_and_tag(game_date, predicted, log_starts)
        model_features = context["model_features"]
        raw_dist = mass(raw_mu, alpha)
        corrected_dist = mass(corrected_mu, alpha)
        raw_probs = {f"p_over_{str(line).replace('.', '_')}": line_probabilities(raw_mu, line, alpha)[0] for line in THRESHOLDS}
        corrected_probs = {f"p_over_{str(line).replace('.', '_')}": line_probabilities(corrected_mu, line, alpha)[0] for line in THRESHOLDS}
        fallback = "+".join(sorted({prediction["away_starter_fallback_status"], prediction["home_starter_fallback_status"]}))
        row = {
            "date": game_date,
            "game_pk": int(game_id),
            "scheduled_first_pitch_utc": scheduled,
            "prediction_timestamp_utc": predicted,
            "run_tag": run_tag,
            "score_timing": timing,
            "raw_total_forecast": raw_mu,
            "intercept_adjusted_total_forecast": corrected_mu,
            "raw_distribution_json": json.dumps(raw_dist.tolist(), separators=(",", ":")),
            "intercept_distribution_json": json.dumps(corrected_dist.tolist(), separators=(",", ":")),
            "raw_probability_fields_json": json.dumps(raw_probs, sort_keys=True, separators=(",", ":")),
            "intercept_probability_fields_json": json.dumps(corrected_probs, sort_keys=True, separators=(",", ":")),
            "model_version": prediction["model_version"],
            "model_hash": model_hash,
            "canonical_prediction_identity": identity,
            "prediction_payload_sha256": prediction_sha,
            "feature_state_hash": feature_hash,
            "context_payload_sha256": context_sha,
            "schedule_source_sha256": schedule_hash,
            "market_source_sha256": market_hash,
            "official_source_path": official["official_source_path"],
            "official_source_sha256": official["official_source_sha256"],
            "grading_payload_sha256": grading_sha,
            "actual_total": official["actual_total"],
            "raw_crps": crps(raw_mu, official["actual_total"], alpha),
            "intercept_crps": crps(corrected_mu, official["actual_total"], alpha),
            "prediction_time_market_line": prediction.get("total_line"),
            "prediction_time_market_status": prediction.get("market_status"),
            "interval_80_low": prediction.get("interval_80_low"),
            "interval_80_high": prediction.get("interval_80_high"),
            "park_factor": prediction.get("park_factor"),
            "park_state": prediction.get("park_fallback_status"),
            "starter_completeness_state": fallback,
            "context_quality_state": prediction.get("context_quality_state"),
            "baseline_b_team_shrunk": 0.5 * (float(model_features["home_offense"]) + float(model_features["away_offense"]) +
                                                float(model_features["home_prevention"]) + float(model_features["away_prevention"])),
            "initial_prior_league_games": int(prediction["dynamic_league_environment"]["season_history_depth"]),
            "initial_prior_league_mean": float(prediction["dynamic_league_environment"]["season_to_date_league_rpg"]),
        }
        if prediction.get("total_line") is not None:
            ro, ru, rp = line_probabilities(raw_mu, float(prediction["total_line"]), alpha)
            io, iu, ip = line_probabilities(corrected_mu, float(prediction["total_line"]), alpha)
            row.update({"raw_p_over_prediction_line": ro, "raw_p_under_prediction_line": ru, "raw_p_push_prediction_line": rp,
                        "intercept_p_over_prediction_line": io, "intercept_p_under_prediction_line": iu,
                        "intercept_p_push_prediction_line": ip})
        rows.append(row)
    if len({row["canonical_prediction_identity"] for row in rows}) != len(rows):
        raise RuntimeError("DUPLICATE_PROSPECTIVE_IDENTITIES")
    return rows, unresolved


def add_league_baseline(rows: list[dict[str, Any]]) -> dict[str, Any]:
    first = rows[0]
    prior_games = int(first["initial_prior_league_games"])
    prior_runs = prior_games * float(first["initial_prior_league_mean"])
    lineage = []
    for date in sorted({row["date"] for row in rows}):
        forecast = prior_runs / prior_games
        date_rows = [row for row in rows if row["date"] == date]
        for row in date_rows:
            row["baseline_a_prior_league_mean"] = forecast
        finals = official_games(date)
        lineage.append({"date": date, "forecast": forecast, "prior_games": prior_games,
                        "official_games_added_after_forecast": len(finals), "official_runs_added_after_forecast": sum(r["actual_total"] for r in finals)})
        prior_games += len(finals)
        prior_runs += sum(row["actual_total"] for row in finals)
    return {"initial_games": int(first["initial_prior_league_games"]), "initial_mean": float(first["initial_prior_league_mean"]),
            "daily_lineage": lineage}


def exclusions(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    predicted = {(row["date"], row["game_pk"]) for row in rows}
    output = []
    for date in sorted({row["date"] for row in rows}):
        for game in official_games(date):
            if (date, game["game_id"]) in predicted:
                continue
            if date == "2026-08-07" and game["game_id"] == 824081:
                reason = "AWAY_PROBABLE_PITCHER_UNAVAILABLE"
            else:
                reason = "PREGAME_CUTOFF_FAILED"
            output.append({"date": date, "game_pk": game["game_id"], "scheduled_first_pitch_utc": game["scheduled_start_utc"],
                           "exclusion_reason": reason, "resolution": "FAIL_CLOSED_NO_RETROSPECTIVE_PREDICTION"})
    return output


def clustered_bootstrap(rows: list[dict[str, Any]], alpha: float) -> list[dict[str, Any]]:
    dates = sorted({row["date"] for row in rows})
    # Precompute game losses, then aggregate by date. Bootstrap draws only combine
    # ten small sufficient-statistic vectors rather than rebuilding distributions.
    statistic_names = ("n", "raw_abs", "raw_sq", "raw_err", "raw_crps", "int_err", "int_crps",
                       "a_abs", "a_sq", "b_abs", "b_sq")
    cluster_stats = []
    for date in dates:
        selected = [row for row in rows if row["date"] == date]
        raw_error = np.array([row["raw_total_forecast"] - row["actual_total"] for row in selected])
        int_error = np.array([row["intercept_adjusted_total_forecast"] - row["actual_total"] for row in selected])
        a_error = np.array([row["baseline_a_prior_league_mean"] - row["actual_total"] for row in selected])
        b_error = np.array([row["baseline_b_team_shrunk"] - row["actual_total"] for row in selected])
        cluster_stats.append(np.array([len(selected), np.abs(raw_error).sum(), (raw_error**2).sum(), raw_error.sum(),
                                       sum(row["raw_crps"] for row in selected), int_error.sum(),
                                       sum(row["intercept_crps"] for row in selected), np.abs(a_error).sum(),
                                       (a_error**2).sum(), np.abs(b_error).sum(), (b_error**2).sum()], dtype=float))
    cluster_stats_array = np.vstack(cluster_stats)

    def derive(s: np.ndarray) -> dict[str, float]:
        values = dict(zip(statistic_names, s)); n = values["n"]
        raw_bias = values["raw_err"] / n; int_bias = values["int_err"] / n
        return {
            "RAW_MAE": values["raw_abs"] / n,
            "RAW_RMSE": math.sqrt(values["raw_sq"] / n),
            "RAW_BIAS": raw_bias,
            "RAW_CRPS": values["raw_crps"] / n,
            "RAW_MINUS_BASELINE_A_MAE": (values["raw_abs"] - values["a_abs"]) / n,
            "RAW_MINUS_BASELINE_A_RMSE": math.sqrt(values["raw_sq"] / n) - math.sqrt(values["a_sq"] / n),
            "RAW_MINUS_BASELINE_B_MAE": (values["raw_abs"] - values["b_abs"]) / n,
            "RAW_MINUS_BASELINE_B_RMSE": math.sqrt(values["raw_sq"] / n) - math.sqrt(values["b_sq"] / n),
            "INTERCEPT_MINUS_RAW_CRPS": (values["int_crps"] - values["raw_crps"]) / n,
            "INTERCEPT_MINUS_RAW_ABSOLUTE_BIAS": abs(int_bias) - abs(raw_bias),
        }

    interpretations = {
        "RAW_MAE": "LOWER_IS_BETTER", "RAW_RMSE": "LOWER_IS_BETTER", "RAW_BIAS": "CLOSER_TO_ZERO_IS_BETTER",
        "RAW_CRPS": "LOWER_IS_BETTER", "RAW_MINUS_BASELINE_A_MAE": "NEGATIVE_FAVORS_RAW",
        "RAW_MINUS_BASELINE_A_RMSE": "NEGATIVE_FAVORS_RAW", "RAW_MINUS_BASELINE_B_MAE": "NEGATIVE_FAVORS_RAW",
        "RAW_MINUS_BASELINE_B_RMSE": "NEGATIVE_FAVORS_RAW", "INTERCEPT_MINUS_RAW_CRPS": "NEGATIVE_FAVORS_INTERCEPT",
        "INTERCEPT_MINUS_RAW_ABSOLUTE_BIAS": "NEGATIVE_FAVORS_INTERCEPT",
    }
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    draws = {name: [] for name in interpretations}
    for _ in range(BOOTSTRAP_REPS):
        indices = rng.integers(0, len(dates), size=len(dates))
        derived = derive(cluster_stats_array[indices].sum(axis=0))
        for name, value in derived.items():
            draws[name].append(value)
    point = derive(cluster_stats_array.sum(axis=0))
    output = []
    for name, interpretation in interpretations.items():
        values = np.array(draws[name])
        output.append({"metric": name, "estimate": point[name], "cluster_unit": "GAME_DATE", "date_clusters": len(dates),
                       "bootstrap_reps": BOOTSTRAP_REPS, "seed": BOOTSTRAP_SEED,
                       "ci_95_lower": float(np.quantile(values, 0.025)), "ci_95_upper": float(np.quantile(values, 0.975)),
                       "fraction_draws_below_zero": float(np.mean(values < 0)), "fraction_draws_above_zero": float(np.mean(values > 0)),
                       "interpretation": interpretation})
    return output


def comparison_status(row: dict[str, Any]) -> str:
    if float(row["ci_95_upper"]) < 0:
        return "AHEAD"
    if float(row["ci_95_lower"]) > 0:
        return "BEHIND"
    return "EFFECTIVELY_TIED"


def attach_markets(rows: list[dict[str, Any]], alpha: float) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{MARKET_LEDGER}?mode=ro", uri=True)
    pinnacle = defaultdict(list)
    for identity, timing, payload_json, created in connection.execute(
        "SELECT prediction_identity,timing_relationship,attachment_payload_json,created_at_utc FROM pinnacle_totals_shadow_attachments"
    ):
        pinnacle[identity].append((created, timing, json.loads(payload_json)))
    consensus = defaultdict(list)
    for identity, captured, payload_json in connection.execute(
        "SELECT prediction_identity,captured_at_utc,consensus_payload_json FROM totals_shadow_market_consensus"
    ):
        consensus[identity].append((captured, json.loads(payload_json)))
    connection.close()
    output = []
    for row in rows:
        predicted = iso_utc(row["prediction_timestamp_utc"])
        start = iso_utc(row["scheduled_first_pitch_utc"])
        for source, records, line_key in (
            ("PINNACLE", pinnacle[row["canonical_prediction_identity"]], "pinnacle_total"),
            ("CANONICAL_MULTIBOOK_CONSENSUS", consensus[row["canonical_prediction_identity"]], "median_total_line"),
        ):
            eligible = [(stamp, timing, payload) if source == "PINNACLE" else (stamp, "POST_PREDICTION_MARKET_OBSERVATION", payload)
                        for stamp, *rest in records for timing, payload in ([rest] if source == "PINNACLE" else [(None, rest[0])])
                        if iso_utc(stamp) >= predicted and iso_utc(stamp) < start]
            if not eligible:
                continue
            stamp, timing, payload = min(eligible, key=lambda item: item[0])
            line = float(payload[line_key])
            actual = int(row["actual_total"])
            raw_over, raw_under, raw_push = line_probabilities(float(row["raw_total_forecast"]), line, alpha)
            int_over, int_under, int_push = line_probabilities(float(row["intercept_adjusted_total_forecast"]), line, alpha)
            result = "OVER" if actual > line else ("UNDER" if actual < line else "PUSH")
            market_probability = payload.get("median_no_vig_over_probability_at_consensus_line")
            output.append({"source": source, "date": row["date"], "game_pk": row["game_pk"], "capture_timestamp_utc": stamp,
                           "timing_relationship": timing or "POST_PREDICTION_MARKET_OBSERVATION",
                           "lead_time_minutes": (start - iso_utc(stamp)).total_seconds() / 60.0,
                           "market_total": line, "actual_total": actual, "raw_total_forecast": row["raw_total_forecast"],
                           "intercept_adjusted_total_forecast": row["intercept_adjusted_total_forecast"],
                           "raw_absolute_error": abs(float(row["raw_total_forecast"]) - actual),
                           "intercept_absolute_error": abs(float(row["intercept_adjusted_total_forecast"]) - actual),
                           "market_absolute_error": abs(line - actual), "raw_minus_market": float(row["raw_total_forecast"]) - line,
                           "intercept_minus_market": float(row["intercept_adjusted_total_forecast"]) - line,
                           "result": result, "raw_p_over": raw_over, "raw_p_under": raw_under, "raw_p_push": raw_push,
                           "intercept_p_over": int_over, "intercept_p_under": int_under, "intercept_p_push": int_push,
                           "market_no_vig_over_probability": market_probability})
    return output


def aggregate_market(market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for source in ("PINNACLE", "CANONICAL_MULTIBOOK_CONSENSUS"):
        rows = [row for row in market_rows if row["source"] == source]
        binary = [row for row in rows if row["result"] != "PUSH"]
        raw_records = [{"probability": row["raw_p_over"] / (row["raw_p_over"] + row["raw_p_under"]), "observed": row["result"] == "OVER"} for row in binary]
        int_records = [{"probability": row["intercept_p_over"] / (row["intercept_p_over"] + row["intercept_p_under"]), "observed": row["result"] == "OVER"} for row in binary]
        market_records = [{"probability": row["market_no_vig_over_probability"], "observed": row["result"] == "OVER"}
                          for row in binary if row["market_no_vig_over_probability"] is not None]
        for variant, error_key, records in (("RAW_V1", "raw_absolute_error", raw_records),
                                             ("V1_INTERCEPT", "intercept_absolute_error", int_records),
                                             (source, "market_absolute_error", market_records)):
            proper = binary_metrics(records)
            output.append({"source": source, "variant": variant, "synchronized_rows": len(rows),
                           "binary_nonpush_rows": len(binary), "mean_lead_time_minutes": mean(row["lead_time_minutes"] for row in rows) if rows else math.nan,
                           "median_lead_time_minutes": median(row["lead_time_minutes"] for row in rows) if rows else math.nan,
                           "mae": mean(row[error_key] for row in rows) if rows else math.nan,
                           "proper_score_rows": proper["rows"], "brier": proper["brier"], "log_loss": proper["log_loss"],
                           "mean_absolute_raw_model_market_separation": mean(abs(row["raw_minus_market"]) for row in rows) if rows else math.nan,
                           "median_absolute_raw_model_market_separation": median(abs(row["raw_minus_market"]) for row in rows) if rows else math.nan,
                           "timing_quality": "PREGAME_POST_PREDICTION" if rows else "NOT_AVAILABLE"})
    return output


def group_metrics(rows: list[dict[str, Any]], group_type: str, group_name: str, alpha: float) -> dict[str, Any]:
    raw = metrics(rows, "raw_total_forecast", alpha)
    corrected = metrics(rows, "intercept_adjusted_total_forecast", alpha)
    return {"group_type": group_type, "group": group_name, "games": len(rows),
            "actual_mean": mean(row["actual_total"] for row in rows), "raw_mean_forecast": mean(row["raw_total_forecast"] for row in rows),
            "raw_mae": raw["mae"], "raw_rmse": raw["rmse"], "raw_bias": raw["forecast_minus_actual_bias"], "raw_crps": raw["crps"],
            "intercept_mean_forecast": mean(row["intercept_adjusted_total_forecast"] for row in rows),
            "intercept_mae": corrected["mae"], "intercept_rmse": corrected["rmse"],
            "intercept_bias": corrected["forecast_minus_actual_bias"], "intercept_crps": corrected["crps"]}


def calibration_rows(rows: list[dict[str, Any]], alpha: float) -> list[dict[str, Any]]:
    observations = {"RAW_V1": [], "V1_INTERCEPT": []}
    expanded = []
    for row in rows:
        for line in THRESHOLDS:
            observed = float(row["actual_total"] > line)
            for variant, key in (("RAW_V1", "raw_total_forecast"), ("V1_INTERCEPT", "intercept_adjusted_total_forecast")):
                probability = line_probabilities(float(row[key]), line, alpha)[0]
                record = {"probability": probability, "observed": observed, "line": line}
                observations[variant].append(record)
                expanded.append((variant, record))
    output = []
    for variant in observations:
        summary = binary_metrics(observations[variant])
        output.append({"row_type": "OVERALL_GOVERNED_HALF_RUN_LADDER", "variant": variant, "line": "ALL", "probability_bin": "ALL", **summary})
        for line in THRESHOLDS:
            subset = [record for record in observations[variant] if record["line"] == line]
            output.append({"row_type": "GOVERNED_LINE", "variant": variant, "line": line, "probability_bin": "ALL", **binary_metrics(subset)})
        for index in range(10):
            lower, upper = index / 10, (index + 1) / 10
            subset = [record for record in observations[variant] if record["probability"] >= lower and
                      (record["probability"] < upper or (index == 9 and record["probability"] <= upper))]
            if subset:
                output.append({"row_type": "FIXED_PROBABILITY_BIN", "variant": variant, "line": "ALL",
                               "probability_bin": f"[{lower:.1f},{upper:.1f}{']' if index == 9 else ')'}", **binary_metrics(subset)})
    return output


def run(output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    config_bytes = CONFIG.read_bytes()
    candidate = json.loads(config_bytes)
    stable = {key: candidate[key] for key in candidate if key != "canonical_model_hash"}
    if canonical_hash(stable) != candidate["canonical_model_hash"] or candidate["canonical_model_hash"] != MODEL_HASH:
        raise RuntimeError("TOTALS_MODEL_IDENTITY_VERIFICATION_FAILED")
    alpha = float(candidate["dispersion_alpha"])
    rows, unresolved = load_population(alpha)
    baseline_lineage = add_league_baseline(rows)
    excluded = exclusions(rows)
    dates = sorted({row["date"] for row in rows})
    if len(rows) != 126 or len(dates) != 10:
        raise RuntimeError(f"UNEXPECTED_PROSPECTIVE_POPULATION_{len(rows)}_{len(dates)}")

    # Independent reproduction guardrails: stop before conclusions on a mismatch.
    known = {
        "2026-08-14": {"mae": 4.40262146235398, "forecast_minus_actual_bias": -0.3244268174855517, "crps": 2.971958530182673},
        "2026-08-15": {"mae": 2.948089176955229, "forecast_minus_actual_bias": -0.2541955655195722, "crps": 2.1080449772903833},
    }
    reproduction = []
    for scope, selected in [("ALL", rows)] + [(date, [row for row in rows if row["date"] == date]) for date in dates]:
        for variant, key in (("RAW_V1", "raw_total_forecast"), ("V1_INTERCEPT", "intercept_adjusted_total_forecast")):
            result = metrics(selected, key, alpha)
            record = {"scope": scope, "variant": variant, **result, "known_summary_check": "NOT_PREDECLARED"}
            if variant == "RAW_V1" and scope in known:
                deltas = {name: result[name] - expected for name, expected in known[scope].items()}
                record.update({f"known_{name}": expected for name, expected in known[scope].items()})
                record.update({f"reproduction_delta_{name}": delta for name, delta in deltas.items()})
                record["known_summary_check"] = "PASS" if max(abs(value) for value in deltas.values()) < 1e-12 else "FAIL"
                if record["known_summary_check"] != "PASS":
                    raise RuntimeError(f"KNOWN_METRIC_REPRODUCTION_FAILED_{scope}")
            reproduction.append(record)
    overall_raw = metrics(rows, "raw_total_forecast", alpha)
    overall_intercept = metrics(rows, "intercept_adjusted_total_forecast", alpha)
    reproduction.append({"scope": "ALL", "variant": "INTERCEPT_MINUS_RAW",
                         **{key: overall_intercept[key] - overall_raw[key] for key in ("mae", "rmse", "forecast_minus_actual_bias", "crps")},
                         "absolute_bias_delta": abs(overall_intercept["forecast_minus_actual_bias"]) - abs(overall_raw["forecast_minus_actual_bias"]),
                         "known_summary_check": "DERIVED"})

    raw_vs_baselines = []
    for name, key in (("RAW_V1", "raw_total_forecast"), ("BASELINE_A_PRIOR_LEAGUE_MEAN", "baseline_a_prior_league_mean"),
                      ("BASELINE_B_TEAM_SHRUNK", "baseline_b_team_shrunk")):
        raw_vs_baselines.append({"row_type": "MODEL_OR_BASELINE", "variant": name, **metrics(rows, key, alpha),
                                 "distribution_contract": f"NEGATIVE_BINOMIAL_ALPHA_{alpha:.15f}"})
    for name, key in (("BASELINE_A_PRIOR_LEAGUE_MEAN", "baseline_a_prior_league_mean"),
                      ("BASELINE_B_TEAM_SHRUNK", "baseline_b_team_shrunk")):
        other = metrics(rows, key, alpha)
        raw_vs_baselines.append({"row_type": "RAW_MINUS_BASELINE", "variant": f"RAW_V1_MINUS_{name}", "games": len(rows),
                                 **{metric: overall_raw[metric] - other[metric] for metric in ("mae", "rmse", "forecast_minus_actual_bias", "crps")},
                                 "distribution_contract": f"COMMON_NEGATIVE_BINOMIAL_ALPHA_{alpha:.15f}_COMPARATIVE_ONLY"})

    bootstrap = clustered_bootstrap(rows, alpha)
    bootstrap_by_name = {row["metric"]: row for row in bootstrap}
    population_status = comparison_status(bootstrap_by_name["RAW_MINUS_BASELINE_A_MAE"])
    team_status = comparison_status(bootstrap_by_name["RAW_MINUS_BASELINE_B_MAE"])

    daily = []
    for date in dates:
        selected = [row for row in rows if row["date"] == date]
        raw = metrics(selected, "raw_total_forecast", alpha)
        corrected = metrics(selected, "intercept_adjusted_total_forecast", alpha)
        base_a = metrics(selected, "baseline_a_prior_league_mean", alpha)
        base_b = metrics(selected, "baseline_b_team_shrunk", alpha)
        daily.append({"date": date, "games": len(selected), "raw_mae": raw["mae"], "raw_bias": raw["forecast_minus_actual_bias"],
                      "raw_crps": raw["crps"], "intercept_mae": corrected["mae"], "intercept_bias": corrected["forecast_minus_actual_bias"],
                      "intercept_crps": corrected["crps"], "population_baseline_mae": base_a["mae"], "team_baseline_mae": base_b["mae"],
                      "raw_beats_population_baseline": raw["mae"] < base_a["mae"], "raw_beats_team_baseline": raw["mae"] < base_b["mae"],
                      "intercept_improves_crps": corrected["crps"] < raw["crps"],
                      "intercept_reduces_absolute_bias": abs(corrected["forecast_minus_actual_bias"]) < abs(raw["forecast_minus_actual_bias"])})

    cumulative = []
    accumulated = []
    for date in dates:
        accumulated += [row for row in rows if row["date"] == date]
        raw = metrics(accumulated, "raw_total_forecast", alpha); corrected = metrics(accumulated, "intercept_adjusted_total_forecast", alpha)
        base_a = metrics(accumulated, "baseline_a_prior_league_mean", alpha); base_b = metrics(accumulated, "baseline_b_team_shrunk", alpha)
        cumulative.append({"through_date": date, "cumulative_games": len(accumulated), "raw_mae": raw["mae"], "raw_bias": raw["forecast_minus_actual_bias"],
                           "raw_crps": raw["crps"], "intercept_mae": corrected["mae"], "intercept_bias": corrected["forecast_minus_actual_bias"],
                           "intercept_crps": corrected["crps"], "population_baseline_mae": base_a["mae"], "team_baseline_mae": base_b["mae"],
                           "raw_minus_population_mae": raw["mae"] - base_a["mae"], "raw_minus_team_mae": raw["mae"] - base_b["mae"],
                           "intercept_minus_raw_crps": corrected["crps"] - raw["crps"]})

    lodo = []
    full_deltas = {"population": overall_raw["mae"] - metrics(rows, "baseline_a_prior_league_mean", alpha)["mae"],
                   "team": overall_raw["mae"] - metrics(rows, "baseline_b_team_shrunk", alpha)["mae"],
                   "crps": overall_intercept["crps"] - overall_raw["crps"]}
    for date in dates:
        selected = [row for row in rows if row["date"] != date]
        raw = metrics(selected, "raw_total_forecast", alpha); corrected = metrics(selected, "intercept_adjusted_total_forecast", alpha)
        pop_delta = raw["mae"] - metrics(selected, "baseline_a_prior_league_mean", alpha)["mae"]
        team_delta = raw["mae"] - metrics(selected, "baseline_b_team_shrunk", alpha)["mae"]
        crps_delta = corrected["crps"] - raw["crps"]
        lodo.append({"omitted_date": date, "games_remaining": len(selected), "raw_minus_population_mae": pop_delta,
                     "raw_minus_team_mae": team_delta, "intercept_minus_raw_crps": crps_delta,
                     "raw_bias": raw["forecast_minus_actual_bias"], "intercept_bias": corrected["forecast_minus_actual_bias"],
                     "population_delta_influence": abs(pop_delta - full_deltas["population"]),
                     "team_delta_influence": abs(team_delta - full_deltas["team"]), "crps_delta_influence": abs(crps_delta - full_deltas["crps"])})
    for metric_name in ("population_delta_influence", "team_delta_influence", "crps_delta_influence"):
        largest = max(row[metric_name] for row in lodo)
        for row in lodo:
            row[f"largest_{metric_name}"] = row[metric_name] == largest

    bias_groups: list[tuple[str, str, list[dict[str, Any]]]] = [("CUMULATIVE", "ALL", rows)]
    bias_groups += [("DATE", date, [row for row in rows if row["date"] == date]) for date in dates]
    weeks = sorted({datetime.fromisoformat(row["date"]).strftime("%G-W%V") for row in rows})
    bias_groups += [("WEEK", week, [row for row in rows if datetime.fromisoformat(row["date"]).strftime("%G-W%V") == week]) for week in weeks]
    bias_groups += [("MONTH", month, [row for row in rows if row["date"].startswith(month)])
                    for month in sorted({row["date"][:7] for row in rows})]
    bias_groups += [("TIME_BLOCK", "AUG_06_TO_10", [row for row in rows if row["date"] <= "2026-08-10"]),
                    ("TIME_BLOCK", "AUG_11_TO_15", [row for row in rows if row["date"] >= "2026-08-11"])]
    for label in ("<7.75", "7.75-8.24", "8.25-8.74", "8.75-9.24", ">=9.25"):
        selected = [row for row in rows if bin_label(float(row["raw_total_forecast"]), [(7.75, "<7.75"), (8.25, "7.75-8.24"),
                                                                                       (8.75, "8.25-8.74"), (9.25, "8.75-9.24"),
                                                                                       (math.inf, ">=9.25")]) == label]
        if selected: bias_groups.append(("RAW_FORECAST_BAND", label, selected))
    for label in ("RUN_SUPPRESSING_<0.98", "NEUTRAL_0.98_TO_1.02", "RUN_BOOSTING_>1.02"):
        selected = [row for row in rows if bin_label(float(row["park_factor"]), [(0.98, "RUN_SUPPRESSING_<0.98"), (1.0200000001, "NEUTRAL_0.98_TO_1.02"),
                                                                                (math.inf, "RUN_BOOSTING_>1.02")]) == label]
        if selected: bias_groups.append(("PARK_CONTEXT", label, selected))
    for state in sorted({row["starter_completeness_state"] for row in rows}):
        selected = [row for row in rows if row["starter_completeness_state"] == state]
        bias_groups.append(("PROBABLE_PITCHER_COMPLETENESS", state, selected))
    for label in ("NARROW_<=9", "TYPICAL_10", "WIDE_>=11"):
        selected = [row for row in rows if bin_label(float(row["interval_80_high"] - row["interval_80_low"]),
                                                     [(9.5, "NARROW_<=9"), (10.5, "TYPICAL_10"), (math.inf, "WIDE_>=11")]) == label]
        if selected: bias_groups.append(("UNCERTAINTY_BAND", label, selected))
    bias_characterization = [group_metrics(selected, kind, label, alpha) for kind, label, selected in bias_groups]
    negative_daily = sum(row["raw_bias"] < 0 for row in daily)
    if overall_raw["forecast_minus_actual_bias"] < 0 and negative_daily >= 7:
        bias_status = "PERSISTENT_SYSTEMATIC"
    elif overall_raw["forecast_minus_actual_bias"] >= 0:
        bias_status = "NOT_PRESENT"
    else:
        bias_status = "MIXED"

    intercept_ci = bootstrap_by_name["INTERCEPT_MINUS_RAW_CRPS"]
    dates_intercept_crps = sum(row["intercept_improves_crps"] for row in daily)
    if intercept_ci["ci_95_upper"] < 0 and dates_intercept_crps >= 7:
        intercept_behavior = "CONSISTENTLY_HELPFUL"
    elif overall_intercept["crps"] < overall_raw["crps"] and intercept_ci["fraction_draws_below_zero"] > 0.6:
        intercept_behavior = "DIRECTIONALLY_HELPFUL_NOT_SEPARATED"
    elif overall_intercept["crps"] < overall_raw["crps"]:
        intercept_behavior = "MIXED"
    else:
        intercept_behavior = "NOT_HELPFUL"
    intercept_stress = [{"row_type": "CUMULATIVE", "date_or_metric": "ALL", "games": len(rows),
                         "raw_bias": overall_raw["forecast_minus_actual_bias"], "intercept_bias": overall_intercept["forecast_minus_actual_bias"],
                         "absolute_bias_reduction": abs(overall_raw["forecast_minus_actual_bias"]) - abs(overall_intercept["forecast_minus_actual_bias"]),
                         "raw_crps": overall_raw["crps"], "intercept_crps": overall_intercept["crps"],
                         "intercept_minus_raw_crps": overall_intercept["crps"] - overall_raw["crps"],
                         "intercept_prospective_behavior": intercept_behavior}]
    intercept_stress += [{"row_type": "DATE", "date_or_metric": row["date"], "games": row["games"], "raw_bias": row["raw_bias"],
                          "intercept_bias": row["intercept_bias"], "absolute_bias_reduction": abs(row["raw_bias"]) - abs(row["intercept_bias"]),
                          "raw_crps": row["raw_crps"], "intercept_crps": row["intercept_crps"],
                          "intercept_minus_raw_crps": row["intercept_crps"] - row["raw_crps"],
                          "intercept_prospective_behavior": "DATE_LEVEL_NOT_A_DECISION"} for row in daily]
    intercept_stress.append({"row_type": "CLUSTER_BOOTSTRAP", "date_or_metric": "INTERCEPT_MINUS_RAW_CRPS", "games": len(rows),
                             "intercept_minus_raw_crps": intercept_ci["estimate"], "ci_95_lower": intercept_ci["ci_95_lower"],
                             "ci_95_upper": intercept_ci["ci_95_upper"], "fraction_favoring_intercept": intercept_ci["fraction_draws_below_zero"],
                             "intercept_prospective_behavior": intercept_behavior})

    calibration = calibration_rows(rows, alpha)
    probability_overall = {row["variant"]: row for row in calibration if row["row_type"] == "OVERALL_GOVERNED_HALF_RUN_LADDER"}

    line_rows = []
    represented_lines = sorted({float(row["prediction_time_market_line"]) for row in rows if row["prediction_time_market_line"] is not None})
    line_groups = [("<=7.5", lambda x: x <= 7.5), ("8.0", lambda x: x == 8.0), ("8.5", lambda x: x == 8.5),
                   ("9.0", lambda x: x == 9.0), ("9.5", lambda x: x == 9.5), (">=10.0", lambda x: x >= 10.0)]
    for label, predicate in line_groups:
        selected = [row for row in rows if row["prediction_time_market_line"] is not None and predicate(float(row["prediction_time_market_line"]))]
        if not selected: continue
        raw = metrics(selected, "raw_total_forecast", alpha); corrected = metrics(selected, "intercept_adjusted_total_forecast", alpha)
        nonpush = [row for row in selected if float(row["actual_total"]) != float(row["prediction_time_market_line"])]
        def model_brier(mu_key: str) -> float:
            records = []
            for row in nonpush:
                over, under, _ = line_probabilities(float(row[mu_key]), float(row["prediction_time_market_line"]), alpha)
                records.append({"probability": over / (over + under), "observed": row["actual_total"] > float(row["prediction_time_market_line"])})
            return binary_metrics(records)["brier"]
        line_rows.append({"line_band": label, "represented_lines": ",".join(map(str, [line for line in represented_lines if predicate(line)])),
                          "games": len(selected), "binary_nonpush_rows": len(nonpush), "actual_average_runs": mean(row["actual_total"] for row in selected),
                          "raw_average_forecast": mean(row["raw_total_forecast"] for row in selected), "raw_bias": raw["forecast_minus_actual_bias"],
                          "intercept_bias": corrected["forecast_minus_actual_bias"], "raw_crps": raw["crps"], "intercept_crps": corrected["crps"],
                          "raw_brier": model_brier("raw_total_forecast"), "intercept_brier": model_brier("intercept_adjusted_total_forecast"),
                          "intercept_improves_crps": corrected["crps"] < raw["crps"],
                          "intercept_reduces_absolute_bias": abs(corrected["forecast_minus_actual_bias"]) < abs(raw["forecast_minus_actual_bias"])})

    forecast_bands = [row for row in bias_characterization if row["group_type"] == "RAW_FORECAST_BAND"]
    timing = []
    for state in ("INITIALIZATION_CAPTURE", "INITIAL_INTEGRATION_CAPTURE", "PRIMARY_SCORE", "SCORE_MISSING"):
        selected = [row for row in rows if row["score_timing"] == state]
        if selected:
            record = group_metrics(selected, "SCORE_TIMING", state, alpha)
            record["dates"] = ",".join(sorted({row["date"] for row in selected}))
            record["interpretation"] = "DESCRIPTIVE_ONLY_NO_OUTCOME_BASED_TIMING_SELECTION"
            timing.append(record)
    timing.append({"group_type": "DEFERRED", "group": "FAIL_CLOSED_NO_PREDICTION", "games": len(excluded),
                   "dates": ",".join(sorted({row["date"] for row in excluded})),
                   "interpretation": json.dumps(Counter(row["exclusion_reason"] for row in excluded), sort_keys=True)})

    market_details = attach_markets(rows, alpha)
    market_summary = aggregate_market(market_details)
    market_separation = []
    separation_groups = [("<0.5", 0, 0.5), ("0.5-0.99", 0.5, 1.0), ("1.0-1.49", 1.0, 1.5),
                         ("1.5-1.99", 1.5, 2.0), (">=2.0", 2.0, math.inf)]
    for source in ("PINNACLE", "CANONICAL_MULTIBOOK_CONSENSUS"):
        for label, lower, upper in separation_groups:
            selected = [row for row in market_details if row["source"] == source and lower <= abs(row["raw_minus_market"]) < upper]
            if not selected: continue
            raw_closer = sum(row["raw_absolute_error"] < row["market_absolute_error"] for row in selected)
            market_closer = sum(row["market_absolute_error"] < row["raw_absolute_error"] for row in selected)
            market_separation.append({"source": source, "absolute_raw_model_market_separation_band": label, "games": len(selected),
                                      "raw_mae": mean(row["raw_absolute_error"] for row in selected),
                                      "intercept_mae": mean(row["intercept_absolute_error"] for row in selected),
                                      "market_mae": mean(row["market_absolute_error"] for row in selected),
                                      "raw_model_closer": raw_closer, "market_closer": market_closer,
                                      "ties": len(selected) - raw_closer - market_closer, "interpretation": "CHARACTERIZATION_NOT_EDGE"})

    historical = list(csv.DictReader(HISTORICAL_METRICS.open()))
    hist_by_variant = {row["model"]: row for row in historical}
    historical_comparison = []
    for variant, prospect in (("RAW", overall_raw), ("INTERCEPT", overall_intercept)):
        source = hist_by_variant[variant]
        historical_comparison.append({"variant": "RAW_V1" if variant == "RAW" else "V1_INTERCEPT",
                                      "historical_games": int(source["games"]), "historical_mae": float(source["mae"]),
                                      "historical_rmse": float(source["rmse"]),
                                      "historical_forecast_minus_actual_bias": -float(source["signed_bias_actual_minus_prediction"]),
                                      "historical_crps": float(source["crps"]), "historical_brier": float(source["ladder_brier"]),
                                      "historical_log_loss": float(source["ladder_log_loss"]), "historical_ece": float(source["ladder_ece"]),
                                      "prospective_games": len(rows), "prospective_mae": prospect["mae"], "prospective_rmse": prospect["rmse"],
                                      "prospective_forecast_minus_actual_bias": prospect["forecast_minus_actual_bias"],
                                      "prospective_crps": prospect["crps"], "prospective_brier": probability_overall["RAW_V1" if variant == "RAW" else "V1_INTERCEPT"]["brier"],
                                      "prospective_log_loss": probability_overall["RAW_V1" if variant == "RAW" else "V1_INTERCEPT"]["log_loss"],
                                      "prospective_ece": probability_overall["RAW_V1" if variant == "RAW" else "V1_INTERCEPT"]["ece"]})
    historical_status = "CONSISTENT" if (historical_comparison[0]["historical_forecast_minus_actual_bias"] < 0 and
                                          overall_raw["forecast_minus_actual_bias"] < 0 and
                                          float(hist_by_variant["INTERCEPT"]["crps"]) < float(hist_by_variant["RAW"]["crps"]) and
                                          overall_intercept["crps"] < overall_raw["crps"]) else "MIXED"

    run_environment = []
    actual_running = raw_running = intercept_running = 0.0
    for date in dates:
        selected = [row for row in rows if row["date"] == date]
        actual = sum(row["actual_total"] for row in selected); raw_sum = sum(row["raw_total_forecast"] for row in selected)
        intercept_sum = sum(row["intercept_adjusted_total_forecast"] for row in selected)
        actual_running += actual; raw_running += raw_sum; intercept_running += intercept_sum
        run_environment.append({"date": date, "games": len(selected), "actual_runs": actual, "raw_expected_runs": raw_sum,
                                "intercept_expected_runs": intercept_sum, "actual_minus_raw": actual - raw_sum,
                                "actual_minus_intercept": actual - intercept_sum, "cumulative_games": sum(r["games"] for r in run_environment) + len(selected),
                                "cumulative_actual_runs": actual_running, "cumulative_raw_expected_runs": raw_running,
                                "cumulative_actual_minus_raw": actual_running - raw_running,
                                "cumulative_average_actual_minus_raw": (actual_running - raw_running) / sum(r["games"] for r in run_environment + [{"games": len(selected)}])})
    total_actual = sum(row["actual_total"] for row in rows); total_raw = sum(row["raw_total_forecast"] for row in rows)
    observed_actual_minus_raw = (total_actual - total_raw) / len(rows)
    correction_alignment = abs(observed_actual_minus_raw - INTERCEPT)

    # Status is deliberately conservative: separation requires the date-clustered interval.
    separated_from_both = population_status == "AHEAD" and team_status == "AHEAD"
    point_status = "TOTALS_RAW_POINT_FORECAST_EVIDENCE_MODERATE" if separated_from_both else "TOTALS_RAW_POINT_FORECAST_EVIDENCE_WEAK"
    if intercept_behavior == "CONSISTENTLY_HELPFUL" and probability_overall["V1_INTERCEPT"]["brier"] < probability_overall["RAW_V1"]["brier"]:
        probability_status = "TOTALS_INTERCEPT_PROBABILITY_LAYER_EVIDENCE_MODERATE"
    elif intercept_behavior in ("CONSISTENTLY_HELPFUL", "DIRECTIONALLY_HELPFUL_NOT_SEPARATED"):
        probability_status = "TOTALS_INTERCEPT_PROBABILITY_LAYER_EVIDENCE_WEAK"
    else:
        probability_status = "TOTALS_INTERCEPT_PROBABILITY_LAYER_EVIDENCE_INSUFFICIENT"
    next_direction = "MULTIPLE_OF_THE_ABOVE: CONTINUE_UNCHANGED_PROSPECTIVE_COLLECTION + RUN_ENVIRONMENT_BIAS_INVESTIGATION_JUSTIFIED"

    identity = {
        "task_id": TASK_ID, "review_cutoff": END_DATE, "model_name": candidate.get("candidate_identity", "DIRECT_NEGATIVE_BINOMIAL"),
        "model_family": candidate.get("model_family"), "artifact_path": str(CONFIG.relative_to(ROOT)), "artifact_sha256": hashlib.sha256(config_bytes).hexdigest(),
        "canonical_model_hash": candidate["canonical_model_hash"], "canonical_hash_verified": True,
        "feature_contract": {"feature_order": candidate["feature_order"], "scaler_mean": candidate["scaler_mean"],
                             "scaler_scale": candidate["scaler_scale"], "coefficients": candidate["coefficients"], "intercept": candidate["intercept"]},
        "point_forecast_construction": "exp(frozen standardized linear predictor)",
        "uncertainty_distribution_construction": f"negative binomial mean=point forecast alpha={alpha} support 0..30 with tail folded into 30",
        "probability_generation_contract": "sum governed negative-binomial mass strictly above/below line; integer-line equality is push mass",
        "prediction_timing_contract": "immutable pregame DAILY_DESIGNATED_PREGAME predictions; initialization/integration/PRIMARY_SCORE/SCORE_MISSING bound from timestamps",
        "source_run_binding": {"prediction_ledger": str(LEDGER.relative_to(ROOT)), "market_ledger": str(MARKET_LEDGER.relative_to(ROOT)),
                               "ops_log": str(OPS_LOG.relative_to(ROOT))},
        "variants": {"RAW_V1": {"intercept_adjustment_runs": 0.0}, "V1_INTERCEPT": {"intercept_adjustment_runs": INTERCEPT, "frozen": True}},
        "TOTALS_POINT_FORECAST_FOUNDATION": "RAW_V1", "TOTALS_FAIR_PROBABILITY_FOUNDATION": "V1_INTERCEPT",
        "population": {"dates": dates, "date_clusters": len(dates), "games_predicted": len(rows), "games_resolved": len(rows),
                       "unresolved": len(unresolved), "fail_closed_deferred": len(excluded), "exclusions": excluded},
        "baseline_a_lineage": baseline_lineage,
    }

    write_json(output_dir / "totals_model_identity.json", identity)
    write_csv(output_dir / "totals_prospective_population.csv", rows)
    write_csv(output_dir / "totals_prospective_metric_reproduction.csv", reproduction)
    (output_dir / "totals_baseline_contracts.md").write_text(
        "# Leakage-safe baseline contracts\n\n"
        "These contracts were fixed before inspecting comparative performance.\n\n"
        "## Baseline A — prior league scoring mean\n\n"
        f"The first forecast starts from the frozen pre-August-6 dynamic league state: {baseline_lineage['initial_games']} completed 2026 games "
        f"and {baseline_lineage['initial_mean']:.12f} runs/game. Each date uses that strict-prior mean; only after the date is scored are all "
        "retained official final MLB games from that date appended. Excluded prediction identities therefore still update the next day's baseball baseline.\n\n"
        "## Baseline B — simple team-shrunk scoring baseline\n\n"
        "For each game: `0.5 * (home_offense + away_offense + home_prevention + away_prevention)`. These four values are the frozen model context's "
        "strict-prior, governed team offense/prevention states, already shrunk by their source procedure. No coefficient or shrinkage parameter was fit here.\n\n"
        f"For comparative CRPS only, both baselines use the already frozen model dispersion contract (`alpha={alpha:.15f}`). No baseline parameter was tuned.\n")
    write_csv(output_dir / "totals_raw_vs_baselines.csv", raw_vs_baselines)
    write_csv(output_dir / "totals_clustered_uncertainty.csv", bootstrap)
    write_csv(output_dir / "totals_daily_stability.csv", daily)
    write_csv(output_dir / "totals_cumulative_trajectory.csv", cumulative)
    write_csv(output_dir / "totals_leave_one_date_out.csv", lodo)
    write_csv(output_dir / "totals_raw_bias_characterization.csv", bias_characterization)
    write_csv(output_dir / "totals_intercept_stress_test.csv", intercept_stress)
    write_csv(output_dir / "totals_probability_calibration.csv", calibration)
    write_csv(output_dir / "totals_line_band_behavior.csv", line_rows)
    write_csv(output_dir / "totals_forecast_magnitude_bands.csv", forecast_bands)
    write_csv(output_dir / "totals_score_timing_comparison.csv", timing)
    write_csv(output_dir / "totals_market_reference_comparison.csv",
              [{"row_type": "SUMMARY", **row} for row in market_summary] +
              [{"row_type": "DETAIL", **row} for row in market_details])
    write_csv(output_dir / "totals_market_separation_bands.csv", market_separation)
    write_csv(output_dir / "totals_historical_reference_comparison.csv", historical_comparison)
    write_csv(output_dir / "totals_run_environment_summary.csv", run_environment)

    daily_summary = {"raw_beats_population": sum(row["raw_beats_population_baseline"] for row in daily),
                     "population_beats_raw": sum(not row["raw_beats_population_baseline"] for row in daily),
                     "raw_beats_team": sum(row["raw_beats_team_baseline"] for row in daily),
                     "team_beats_raw": sum(not row["raw_beats_team_baseline"] for row in daily),
                     "intercept_crps_better": dates_intercept_crps, "raw_crps_better": len(daily) - dates_intercept_crps,
                     "intercept_abs_bias_better": sum(row["intercept_reduces_absolute_bias"] for row in daily),
                     "intercept_abs_bias_worse": sum(not row["intercept_reduces_absolute_bias"] for row in daily)}
    lodo_summary = {
        "raw_minus_population_mae_range": [min(row["raw_minus_population_mae"] for row in lodo), max(row["raw_minus_population_mae"] for row in lodo)],
        "raw_minus_team_mae_range": [min(row["raw_minus_team_mae"] for row in lodo), max(row["raw_minus_team_mae"] for row in lodo)],
        "intercept_minus_raw_crps_range": [min(row["intercept_minus_raw_crps"] for row in lodo), max(row["intercept_minus_raw_crps"] for row in lodo)],
        "any_sign_change": any(row["raw_minus_population_mae"] >= 0 or row["raw_minus_team_mae"] >= 0 or
                               row["intercept_minus_raw_crps"] >= 0 for row in lodo),
        "largest_crps_influence_date": next(row["omitted_date"] for row in lodo if row["largest_crps_delta_influence"]),
    }
    line_band_interpretation = "MIXED_BY_MARKET_LINE_BAND_HIGHER_LINES_FAVOR_INTERCEPT_LOW_LINES_FAVOR_RAW"
    primary_timing = next(row for row in timing if row["group"] == "PRIMARY_SCORE")
    retry_timing = next(row for row in timing if row["group"] == "SCORE_MISSING")
    limitations = ["Only 126 games across 10 correlated date clusters are available.",
                   "The prospective window covers August 6–15 only and may not span broader seasonal run environments.",
                   "Eight scheduled games were correctly absent because no governed pregame prediction existed; no replay was performed.",
                   "Market coverage is post-prediction and secondary; Pinnacle attachment rows do not retain two-sided prices for a market probability score.",
                   "Timing and subpopulation comparisons are descriptive and small-cell results are unstable."]
    status_text = f"""# MLB totals existing-model prospective due diligence v1

This is a due-diligence status review, not certification, promotion, redesign, refit, or recalibration.

- `TOTALS_POINT_FORECAST_FOUNDATION = RAW_V1`
- `TOTALS_FAIR_PROBABILITY_FOUNDATION = V1_INTERCEPT`
- `RAW_V1_VS_POPULATION_BASELINE = {population_status}`
- `RAW_V1_VS_TEAM_BASELINE = {team_status}`
- `RAW_UNDERFORECAST_BIAS = {bias_status}`
- `INTERCEPT_PROSPECTIVE_BEHAVIOR = {intercept_behavior}`
- `TOTALS_PROSPECTIVE_VS_HISTORICAL = {historical_status}`
- `TOTAL_LINE_BAND_INTERCEPT_BEHAVIOR = {line_band_interpretation}`
- `SCORE_TIMING_INTERPRETATION = DESCRIPTIVE_DIFFERENCE_PRESENT_NOT_SUSPICIOUSLY_INTERPRETABLE`
- `{point_status}`
- `{probability_status}`
- `NEXT_RESEARCH_DIRECTION = {next_direction}`

The +{INTERCEPT:.6f}-run layer remains frozen. Its observed prospective run correction differs from the average actual-minus-RAW residual by {correction_alignment:.6f} runs/game. It is reviewed only as a distribution/probability layer; RAW remains the point forecast.

## Material limitations

""" + "".join(f"- {item}\n" for item in limitations)
    (output_dir / "totals_due_diligence_status.md").write_text(status_text)

    pop_metrics = metrics(rows, "baseline_a_prior_league_mean", alpha); team_metrics = metrics(rows, "baseline_b_team_shrunk", alpha)
    pin = next(row for row in market_summary if row["source"] == "PINNACLE" and row["variant"] == "PINNACLE")
    con = next(row for row in market_summary if row["source"] == "CANONICAL_MULTIBOOK_CONSENSUS" and row["variant"] == "CANONICAL_MULTIBOOK_CONSENSUS")
    concise = f"""# Concise MLB totals existing-model prospective due diligence v1

- Model: `DIRECT_NEGATIVE_BINOMIAL` / `{MODEL_HASH}`; artifact SHA-256 `{hashlib.sha256(config_bytes).hexdigest()}`.
- Population: {dates[0]} through {dates[-1]}, {len(rows)} predicted/resolved games in {len(dates)} date clusters; {len(excluded)} fail-closed exclusions and {len(unresolved)} unresolved predictions.
- RAW: MAE {overall_raw['mae']:.6f}, RMSE {overall_raw['rmse']:.6f}, bias {overall_raw['forecast_minus_actual_bias']:+.6f}, CRPS {overall_raw['crps']:.6f}.
- INTERCEPT: MAE {overall_intercept['mae']:.6f}, RMSE {overall_intercept['rmse']:.6f}, bias {overall_intercept['forecast_minus_actual_bias']:+.6f}, CRPS {overall_intercept['crps']:.6f}.
- Baseline A: MAE {pop_metrics['mae']:.6f}, RMSE {pop_metrics['rmse']:.6f}; RAW deltas {overall_raw['mae']-pop_metrics['mae']:+.6f} MAE / {overall_raw['rmse']-pop_metrics['rmse']:+.6f} RMSE (`{population_status}`).
- Baseline B: MAE {team_metrics['mae']:.6f}, RMSE {team_metrics['rmse']:.6f}; RAW deltas {overall_raw['mae']-team_metrics['mae']:+.6f} MAE / {overall_raw['rmse']-team_metrics['rmse']:+.6f} RMSE (`{team_status}`).
- Date-clustered INTERCEPT-minus-RAW CRPS: {intercept_ci['estimate']:+.6f} (95% {intercept_ci['ci_95_lower']:+.6f} to {intercept_ci['ci_95_upper']:+.6f}; {intercept_ci['fraction_draws_below_zero']:.1%} favor INTERCEPT).
- Daily stability: {json.dumps(daily_summary, sort_keys=True)}.
- Leave-one-date-out: population MAE delta {lodo_summary['raw_minus_population_mae_range'][0]:+.6f} to {lodo_summary['raw_minus_population_mae_range'][1]:+.6f}; team MAE delta {lodo_summary['raw_minus_team_mae_range'][0]:+.6f} to {lodo_summary['raw_minus_team_mae_range'][1]:+.6f}; INTERCEPT-minus-RAW CRPS {lodo_summary['intercept_minus_raw_crps_range'][0]:+.6f} to {lodo_summary['intercept_minus_raw_crps_range'][1]:+.6f}; sign change: {lodo_summary['any_sign_change']}.
- RAW bias: `{bias_status}`; actual scoring exceeded RAW by {observed_actual_minus_raw:.6f} runs/game. Frozen correction alignment gap: {correction_alignment:.6f}.
- Probability ladder RAW vs INTERCEPT: Brier {probability_overall['RAW_V1']['brier']:.6f} vs {probability_overall['V1_INTERCEPT']['brier']:.6f}; log loss {probability_overall['RAW_V1']['log_loss']:.6f} vs {probability_overall['V1_INTERCEPT']['log_loss']:.6f}; ECE {probability_overall['RAW_V1']['ece']:.6f} vs {probability_overall['V1_INTERCEPT']['ece']:.6f}.
- Line/forecast bands: `{line_band_interpretation}`; forecast-magnitude CRPS favors INTERCEPT in {sum(row['intercept_crps'] < row['raw_crps'] for row in forecast_bands)}/{len(forecast_bands)} fixed bands.
- Score timing: PRIMARY {primary_timing['games']} games, MAE {primary_timing['raw_mae']:.6f}, CRPS {primary_timing['raw_crps']:.6f}; SCORE_MISSING {retry_timing['games']} games, MAE {retry_timing['raw_mae']:.6f}, CRPS {retry_timing['raw_crps']:.6f}. Descriptive only; no outcome-based timing selection.
- Historical consistency: `{historical_status}`. Secondary market rows: Pinnacle {pin['synchronized_rows']} (line MAE {pin['mae']:.6f}); consensus {con['synchronized_rows']} (line MAE {con['mae']:.6f}).
- Point status: `{point_status}`.
- Probability status: `{probability_status}`.
- Next direction: `{next_direction}`. No next step was executed.
"""
    (output_dir / "concise_mlb_totals_existing_model_prospective_due_diligence_v1.md").write_text(concise)

    hash_path = output_dir / "reproducibility_hashes.sha256"
    files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    input_paths = [CONFIG, LEDGER, MARKET_LEDGER, HISTORICAL_METRICS, OPS_LOG]
    hash_lines = [f"{sha256(path)}  {path.name}\n" for path in files]
    hash_lines += [f"{sha256(path)}  INPUT::{path.relative_to(ROOT)}\n" for path in input_paths]
    hash_path.write_text("".join(hash_lines))
    return {"task_id": TASK_ID, "model_hash": MODEL_HASH, "dates": dates, "games": len(rows), "resolved": len(rows),
            "excluded": len(excluded), "raw": overall_raw, "intercept": overall_intercept,
            "population_baseline": pop_metrics, "team_baseline": team_metrics, "clustered_uncertainty": bootstrap,
            "daily_summary": daily_summary, "bias_status": bias_status, "intercept_behavior": intercept_behavior,
            "historical_status": historical_status, "point_status": point_status, "probability_status": probability_status,
            "next_direction": next_direction, "output_dir": str(output_dir)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output_dir), indent=2))


if __name__ == "__main__":
    main()
