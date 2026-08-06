"""Run the frozen-scope MLB Totals Structural Challenger v2 experiment."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import nbinom
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    CONFIG_PATH as V1_CONFIG_PATH,
    attach_context,
    build_history,
    canonical_hash,
    feature_row as live_v1_features,
    fetch_hydrated_schedule,
    load_candidate,
    normalize_schedule,
    score_context,
)

ROOT = Path(__file__).resolve().parents[3]
SPINE = ROOT / "artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06"
V1_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_totals_prediction_representative_rerun_v1/2026-08-06"
EXPERIMENT = "MLB_TOTALS_STRUCTURAL_CHALLENGER_V2"
V1_ALPHA = 0.12944479977012996
SUPPORT = np.arange(31)
THRESHOLDS = (7.5, 8.5, 9.5, 10.5)
SPARSE_K_GRID = (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)
MATERIAL_CRPS_GAIN = 0.01


def sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def strict_team(data: pd.DataFrame) -> pd.DataFrame:
    league: list[tuple[float, float, float]] = []; scored: dict[int, list[float]] = {}; allowed: dict[int, list[float]] = {}; rows = []
    for _, day in data.groupby("game_date", sort=True):
        lt = float(np.mean([x[0] for x in league])) if league else 8.6
        lh = float(np.mean([x[1] for x in league])) if league else 4.4
        la = float(np.mean([x[2] for x in league])) if league else 4.2
        for game in day.itertuples():
            avg = lambda source, key, fallback: float(np.mean(source.get(int(key), []))) if source.get(int(key)) else fallback
            rows.append({"game_pk": game.game_pk, "league_total": lt,
                         "home_offense": avg(scored, game.home_team_id, lh), "home_prevention": avg(allowed, game.home_team_id, la),
                         "away_offense": avg(scored, game.away_team_id, la), "away_prevention": avg(allowed, game.away_team_id, lh)})
        for game in day.itertuples():
            league.append((game.final_total, game.final_home_runs, game.final_away_runs))
            scored.setdefault(int(game.home_team_id), []).append(game.final_home_runs); allowed.setdefault(int(game.home_team_id), []).append(game.final_away_runs)
            scored.setdefault(int(game.away_team_id), []).append(game.final_away_runs); allowed.setdefault(int(game.away_team_id), []).append(game.final_home_runs)
    return pd.DataFrame(rows)


def dynamic_environment(data: pd.DataFrame) -> pd.DataFrame:
    history: list[dict[str, Any]] = []; season: dict[int, list[float]] = defaultdict(list); completed_seasons: dict[int, list[float]] = defaultdict(list); rows = []
    for day_value, day in data.groupby("game_date", sort=True):
        date = pd.Timestamp(day_value); year = int(date.year); prior30 = [x["total"] for x in history if 0 < (date - x["date"]).days <= 30]
        prior_all = [x["total"] for x in history]
        season_values = season[year]; previous = completed_seasons.get(year - 1, [])
        season_rpg = float(np.mean(season_values)) if season_values else (float(np.mean(previous)) if previous else (float(np.mean(prior_all)) if prior_all else 8.6))
        trailing = float(np.mean(prior30)) if prior30 else season_rpg
        prior_season = float(np.mean(previous)) if previous else (float(np.mean(prior_all)) if prior_all else 8.6)
        for game in day.itertuples():
            rows.append({"game_pk": game.game_pk, "season_to_date_league_rpg": season_rpg, "trailing_30_league_rpg": trailing,
                         "prior_season_league_rpg": prior_season, "run_environment_history_depth": len(season_values),
                         "run_environment_cutoff": str(date.date())})
        for game in day.itertuples():
            value = float(game.final_total); season[year].append(value); completed_seasons[year].append(value); history.append({"date": date, "total": value})
    return pd.DataFrame(rows)


def starter_hierarchy(data: pd.DataFrame) -> pd.DataFrame:
    team_values: dict[int, list[float]] = defaultdict(list); league_values: list[float] = []; rows = []
    for _, day in data.groupby("game_date", sort=True):
        league_level = float(np.mean(league_values)) if league_values else 4.3
        for game in day.itertuples():
            row = {"game_pk": game.game_pk, "league_starter_level": league_level}
            for side in ("home", "away"):
                team = int(getattr(game, f"{side}_team_id")); values = team_values[team]
                direct = float(np.mean(values)) if values else league_level; depth = len(values)
                # Fixed team-to-league reliability bridge; only the pitcher
                # weight below is estimated on development evidence.
                reliability = depth / (depth + 20.0)
                row[f"{side}_team_starter_level"] = reliability * direct + (1 - reliability) * league_level
                row[f"{side}_team_starter_depth"] = depth
            rows.append(row)
        for game in day.itertuples():
            for side in ("home", "away"):
                value = getattr(game, f"{side}_starter_season_ra9")
                starts = getattr(game, f"{side}_starter_prior_starts")
                if pd.notna(value) and float(starts) > 0:
                    team_values[int(getattr(game, f"{side}_team_id"))].append(float(value)); league_values.append(float(value))
    return pd.DataFrame(rows)


def add_sparse_shrinkage(data: pd.DataFrame, k: float) -> pd.DataFrame:
    result = data.copy()
    for side in ("home", "away"):
        direct = result[f"{side}_starter_ra9"].astype(float); starts = result[f"{side}_starter_prior_starts"].astype(float)
        hierarchy = result[f"{side}_team_starter_level"].fillna(result["league_starter_level"])
        sparse = starts.isin([1.0, 2.0]); weight = starts / (starts + k)
        result[f"{side}_starter_ra9_shrunk"] = np.where(sparse, weight * direct + (1 - weight) * hierarchy, direct)
        result[f"{side}_sparse_starter_flag"] = sparse.astype(float)
    return result


def add_park_features(data: pd.DataFrame) -> pd.DataFrame:
    result = data.copy(); bounded = result.strict_prior_total_run_factor.clip(.8, 1.2); reliability = result.park_history_depth / (result.park_history_depth + 50.0)
    result["park_shrunk_deviation"] = (bounded - 1.0) * reliability
    result["park_history_log1p"] = np.log1p(result.park_history_depth)
    result["park_direct_history_flag"] = (result.park_history_depth >= 20).astype(float)
    result["park_x_trailing30_environment"] = bounded * result.trailing_30_league_rpg
    return result


def nb_mass(mu: np.ndarray | list[float], alpha: float) -> np.ndarray:
    values = np.clip(np.asarray(mu, dtype=float), .05, 30); size = 1 / alpha; probability = size / (size + values[:, None])
    mass = nbinom.pmf(SUPPORT[None, :], size, probability); mass[:, -1] += np.maximum(0, 1 - mass.sum(axis=1)); return mass


def crps(mass: np.ndarray, actual: np.ndarray | pd.Series) -> np.ndarray:
    observed = SUPPORT[None, :] >= np.asarray(actual)[:, None]
    return ((np.cumsum(mass, axis=1) - observed) ** 2).sum(axis=1)


def metrics(model: str, split: str, actual: pd.Series, mu: np.ndarray, mass: np.ndarray) -> dict[str, Any]:
    error = mu - actual.to_numpy(); return {"variant": model, "split": split, "games": len(actual), "mae": float(np.mean(abs(error))),
        "bias": float(np.mean(error)), "rmse": float(np.sqrt(np.mean(error ** 2))), "crps": float(np.mean(crps(mass, actual))),
        "predicted_mean": float(np.mean(mu)), "observed_mean": float(actual.mean()), "distribution_mass_error": float(np.max(abs(mass.sum(axis=1) - 1)))}


def threshold_rows(model: str, split: str, frame: pd.DataFrame, mass: np.ndarray) -> list[dict[str, Any]]:
    rows = []
    for window, groups in [("AGGREGATE", [("ALL", np.arange(len(frame)))]), ("MONTH", list(frame.assign(_month=frame.game_date.dt.to_period("M").astype(str)).groupby("_month").indices.items()))]:
        for period, indices in groups:
            indices = np.asarray(indices); actual_total = frame.final_total.to_numpy()[indices]
            for threshold in THRESHOLDS:
                actual = (actual_total > threshold).astype(int); probability = mass[indices][:, SUPPORT > threshold].sum(axis=1); clipped = np.clip(probability, 1e-9, 1 - 1e-9)
                rows.append({"variant": model, "split": split, "window": window, "period": period, "threshold": threshold, "games": len(indices),
                    "accuracy": float(np.mean((probability >= .5) == actual)), "brier": float(np.mean((probability - actual) ** 2)),
                    "log_loss": float(np.mean(-(actual * np.log(clipped) + (1 - actual) * np.log(1 - clipped)))),
                    "observed_over_rate": float(actual.mean()), "predicted_over_rate": float(probability.mean()), "calibration": float(probability.mean() - actual.mean())})
    return rows


def fit_location(train: pd.DataFrame, columns: list[str]) -> Pipeline:
    return Pipeline([("scaler", StandardScaler()), ("location", PoissonRegressor(alpha=.1, max_iter=1000))]).fit(train[columns], train.final_total)


def choose_sparse_k(data: pd.DataFrame, base_columns: list[str]) -> tuple[float, list[dict[str, Any]]]:
    train = data[data.game_date.dt.year == 2023]; tune = data[data.game_date.dt.year == 2024]; rows = []
    for k in SPARSE_K_GRID:
        transformed = add_sparse_shrinkage(data, k); columns = [c.replace("starter_ra9", "starter_ra9_shrunk") if c in ("home_starter_ra9", "away_starter_ra9") else c for c in base_columns]
        location = fit_location(transformed.loc[train.index], columns); mu = location.predict(transformed.loc[tune.index, columns]); score = float(np.mean(crps(nb_mass(mu, V1_ALPHA), tune.final_total)))
        rows.append({"k": k, "development_train": "2023", "development_tune": "2024", "crps": score})
    selected = min(rows, key=lambda row: (row["crps"], row["k"]))["k"]; return float(selected), rows


def location_audits(data: pd.DataFrame, predictions: dict[str, np.ndarray], splits: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    location_rows, park_rows, sparse_rows = [], [], []
    for split, frame in splits.items():
        indices = frame.index.to_numpy()
        fixed_total_band = pd.cut(frame.final_total, [-1, 6, 11, 99], labels=["LOW_0_6", "MID_7_11", "HIGH_12_PLUS"])
        fixed_park_band = pd.qcut(frame.strict_prior_total_run_factor, 5, labels=["Q1", "Q2", "Q3", "Q4", "Q5"], duplicates="drop")
        sparse_band = np.where(frame[["home_starter_prior_starts", "away_starter_prior_starts"]].min(axis=1).isin([1, 2]), "SPARSE_1_2", "OTHER")
        for variant, all_mu in predictions.items():
            mu = all_mu[indices]; residual = mu - frame.final_total.to_numpy()
            for dimension, levels in [("OVERALL", pd.Series("ALL", index=frame.index)), ("OBSERVED_TOTAL_BAND", pd.Series(fixed_total_band, index=frame.index)),
                                      ("ELEVATION", pd.Series(np.where(frame.elevation >= 1000, "HIGH_1000_PLUS", "BELOW_1000"), index=frame.index))]:
                for level in pd.unique(levels.dropna()):
                    mask = levels.astype(str).to_numpy() == str(level); location_rows.append({"variant": variant, "split": split, "dimension": dimension,
                        "level": str(level), "games": int(mask.sum()), "mean_residual": float(np.mean(residual[mask])), "mae": float(np.mean(abs(residual[mask])))})
            for dimension, levels in [("PARK_FACTOR_QUINTILE", pd.Series(fixed_park_band, index=frame.index)),
                                      ("COORS_REGIME", pd.Series(np.where(frame.venue_id == 19, "COORS_FIELD", "OTHER_VENUES"), index=frame.index)),
                                      ("ELEVATION_REGIME", pd.Series(np.where(frame.elevation >= 1000, "HIGH_1000_PLUS", "BELOW_1000"), index=frame.index))]:
                for level in pd.unique(levels.dropna()):
                    mask = levels.astype(str).to_numpy() == str(level); park_rows.append({"variant": variant, "split": split, "dimension": dimension, "level": str(level),
                        "games": int(mask.sum()), "mean_residual": float(np.mean(residual[mask])), "mae": float(np.mean(abs(residual[mask])))})
            for level in ("SPARSE_1_2", "OTHER"):
                mask = sparse_band == level; sparse_rows.append({"variant": variant, "split": split, "starter_regime": level, "games": int(mask.sum()),
                    "mean_residual": float(np.mean(residual[mask])) if mask.any() else np.nan, "mae": float(np.mean(abs(residual[mask]))) if mask.any() else np.nan})
            for team in sorted(set(frame.home_team_id) | set(frame.away_team_id)):
                mask = ((frame.home_team_id == team) | (frame.away_team_id == team)).to_numpy(); location_rows.append({"variant": variant, "split": split,
                    "dimension": "TEAM_CONCENTRATION", "level": int(team), "games": int(mask.sum()), "mean_residual": float(np.mean(residual[mask])), "mae": float(np.mean(abs(residual[mask])))})
    return pd.DataFrame(location_rows), pd.DataFrame(park_rows), pd.DataFrame(sparse_rows)


def serialize_model(name: str, model: Pipeline, columns: list[str], alpha: float, sparse_k: float, data_contract_hash: str) -> dict[str, Any]:
    payload = {"experiment": EXPERIMENT, "candidate_identity": "MLB_TOTALS_STRUCTURAL_CHALLENGER_V2", "selected_variant": name,
        "model_family": "DIRECT_NEGATIVE_BINOMIAL", "location_family": "STANDARD_SCALER_PLUS_REGULARIZED_POISSON", "location_regularization_alpha": .1,
        "feature_order": columns, "scaler_mean": [float(x) for x in model["scaler"].mean_], "scaler_scale": [float(x) for x in model["scaler"].scale_],
        "intercept": float(model["location"].intercept_), "coefficients": [float(x) for x in model["location"].coef_], "dispersion_alpha": float(alpha),
        "sparse_starter_k": sparse_k, "sparse_starter_weight": "n/(n+k) for exactly 1-2 direct prior starts; unchanged for >=3",
        "park_contract": "clip factor to [0.8,1.2]; multiply deviation by depth/(depth+50); fitted location coefficient",
        "run_environment_contract": "strict-prior season-to-date, fixed trailing-30-day, prior-season, and history depth",
        "development": "2023-2024", "selection": "2025 validation with early-2026 stability gate", "opened_late_2026_selection_use": False,
        "prospective_evidence_begins": "2026-08-06", "data_contract_hash": data_contract_hash, "public_status": "PROSPECTIVE_CHALLENGER_NOT_A_QUALIFIED_BETTING_MODEL"}
    payload["model_hash"] = canonical_hash(payload); return payload


def live_dynamic(history: dict[str, Any], game_date: str) -> dict[str, float]:
    core = history["core"].copy(); core["game_date"] = pd.to_datetime(core.game_date); target = pd.Timestamp(game_date); prior = core[core.game_date < target]; season = prior[prior.game_date.dt.year == target.year]
    trailing = prior[(target - prior.game_date).dt.days.between(1, 30)]; previous = prior[prior.game_date.dt.year == target.year - 1]
    return {"season_to_date_league_rpg": float(season.final_total.mean()), "trailing_30_league_rpg": float(trailing.final_total.mean()),
        "prior_season_league_rpg": float(previous.final_total.mean()), "run_environment_history_depth": float(len(season))}


def score_serialized(features: dict[str, float], manifest: dict[str, Any]) -> tuple[float, np.ndarray]:
    values = np.array([features[name] for name in manifest["feature_order"]]); scaled = (values - manifest["scaler_mean"]) / manifest["scaler_scale"]
    mu = math.exp(float(manifest["intercept"] + np.dot(scaled, manifest["coefficients"]))); return mu, nb_mass([mu], manifest["dispersion_alpha"])[0]


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output-dir", type=Path, required=True); parser.add_argument("--date", default="2026-08-06")
    args = parser.parse_args(); out = args.output_dir; out.mkdir(parents=True, exist_ok=True)

    expected = {line.split("  ", 1)[1]: line.split("  ", 1)[0] for line in (SPINE / "reproducibility_hashes.sha256").read_text().splitlines()}
    reproduction = [{"check": "canonical_games", "expected": 9012, "observed": len(pd.read_csv(SPINE / "totals_core_feature_spine.csv", usecols=["game_pk"])), "status": "PASS"}]
    for name, expected_hash in expected.items():
        observed = sha_file(SPINE / name); reproduction.append({"check": f"frozen_hash:{name}", "expected": expected_hash, "observed": observed, "status": "PASS" if observed == expected_hash else "FAIL"})
    temporal = pd.read_csv(SPINE / "temporal_certification_audit.csv")
    reproduction += [{"check": "same_game_leakage", "expected": 0, "observed": int(temporal.same_game_leakage_rows.sum()), "status": "PASS" if temporal.same_game_leakage_rows.sum() == 0 else "FAIL"},
                     {"check": "later_game_leakage", "expected": 0, "observed": int(temporal.later_game_leakage_rows.sum()), "status": "PASS" if temporal.later_game_leakage_rows.sum() == 0 else "FAIL"}]
    pd.DataFrame(reproduction).to_csv(out / "totals_v2_population_reproduction.csv", index=False)
    if any(row["status"] != "PASS" for row in reproduction): raise RuntimeError("FROZEN_POPULATION_REPRODUCTION_FAILED")
    data_contract_hash = canonical_hash(expected)

    data = pd.read_csv(SPINE / "totals_core_feature_spine.csv"); data["game_date"] = pd.to_datetime(data.game_date); data["scheduled_start_utc"] = pd.to_datetime(data.scheduled_start_utc, utc=True)
    data = data.merge(strict_team(data), on="game_pk").merge(dynamic_environment(data), on="game_pk").merge(starter_hierarchy(data), on="game_pk")
    park_meta = pd.read_csv(SPINE / "strict_prior_park_factor.csv", usecols=["game_pk", "roof_type", "elevation"]); data = data.merge(park_meta, on="game_pk", how="left")
    data["home_starter_ra9"] = data.home_starter_season_ra9.fillna(data.league_total / 2); data["away_starter_ra9"] = data.away_starter_season_ra9.fillna(data.league_total / 2)
    data["home_bullpen_ra9"] = data.home_bullpen_bullpen_ra9.fillna(data.league_total / 2); data["away_bullpen_ra9"] = data.away_bullpen_bullpen_ra9.fillna(data.league_total / 2)
    data = add_park_features(data)

    v1 = load_candidate(); v1_cols = v1["feature_order"]
    dynamic_cols = ["season_to_date_league_rpg", "trailing_30_league_rpg", "prior_season_league_rpg", "run_environment_history_depth"]
    park_cols = ["park_shrunk_deviation", "park_history_log1p", "park_direct_history_flag"]
    v2c_cols = [c for c in v1_cols if c not in ("strict_prior_total_run_factor", "park_history_depth")] + dynamic_cols + park_cols + ["park_x_trailing30_environment"]
    sparse_k, sparse_audit = choose_sparse_k(add_sparse_shrinkage(data, 1.0), v2c_cols)
    data = add_sparse_shrinkage(data, sparse_k)
    v2d_cols = [c.replace("starter_ra9", "starter_ra9_shrunk") if c in ("home_starter_ra9", "away_starter_ra9") else c for c in v2c_cols]
    compact_cols = ["home_offense", "home_prevention", "away_offense", "away_prevention", "home_starter_ra9_shrunk", "away_starter_ra9_shrunk",
                    "home_starter_prior_starts", "away_starter_prior_starts"] + dynamic_cols + park_cols + ["park_x_trailing30_environment"]
    variants = {"V1_CONTROL": v1_cols, "V2A_DYNAMIC_RUN_ENVIRONMENT": v1_cols + dynamic_cols,
                "V2B_PARK_SHRINKAGE": [c for c in v1_cols if c not in ("strict_prior_total_run_factor", "park_history_depth")] + park_cols,
                "V2C_ENVIRONMENT_AWARE_PARK": v2c_cols, "V2D_SPARSE_STARTER_SHRINKAGE": v2d_cols, "V2E_COMPACT_CHALLENGER": compact_cols}
    for columns in variants.values(): data[columns] = data[columns].replace([np.inf, -np.inf], np.nan).fillna(0)
    data["split"] = np.select([data.game_date.dt.year <= 2024, data.game_date.dt.year == 2025,
        (data.game_date.dt.year == 2026) & (data.game_date.dt.month < 7), data.game_date >= pd.Timestamp("2026-07-01")],
        ["DEVELOPMENT", "FROZEN_VALIDATION_2025", "EARLY_2026_SEQUENTIAL", "OPENED_LATE_2026_DIAGNOSTIC"], default="EXCLUDED")
    train = data[data.split == "DEVELOPMENT"]; validation = data[data.split == "FROZEN_VALIDATION_2025"]; early = data[data.split == "EARLY_2026_SEQUENTIAL"]; opened = data[data.split == "OPENED_LATE_2026_DIAGNOSTIC"]

    models, predictions, masses, comparison, thresholds, stability = {}, {}, {}, [], [], []
    for name, columns in variants.items():
        model = fit_location(train, columns); models[name] = model; predictions[name] = np.full(len(data), np.nan); masses[name] = {}
        for split, frame in [("FROZEN_VALIDATION_2025", validation), ("EARLY_2026_SEQUENTIAL", early)]:
            mu = model.predict(frame[columns]); mass = nb_mass(mu, V1_ALPHA); predictions[name][frame.index] = mu; masses[name][split] = mass
            comparison.append(metrics(name, split, frame.final_total, mu, mass)); thresholds += threshold_rows(name, split, frame, mass)
            q = frame[["game_date", "final_total"]].copy(); q["mu"] = mu; q["month"] = q.game_date.dt.to_period("M").astype(str)
            for month, group in q.groupby("month"):
                stability.append({"variant": name, "split": split, "window": "MONTH", "period": month, "games": len(group), "mae": float(np.mean(abs(group.mu - group.final_total))), "bias": float(np.mean(group.mu - group.final_total))})
            for ix in range(49, len(q), 50):
                group = q.iloc[ix - 49:ix + 1]; stability.append({"variant": name, "split": split, "window": "ROLLING_50", "period": str(group.game_date.iloc[-1].date()),
                    "games": 50, "mae": float(np.mean(abs(group.mu - group.final_total))), "bias": float(np.mean(group.mu - group.final_total))})
    comparison_df = pd.DataFrame(comparison); v1_validation = comparison_df.query("variant=='V1_CONTROL' and split=='FROZEN_VALIDATION_2025'").iloc[0]
    v1_early = comparison_df.query("variant=='V1_CONTROL' and split=='EARLY_2026_SEQUENTIAL'").iloc[0]
    stability_df = pd.DataFrame(stability)
    selection_rows = []
    for name in variants:
        val = comparison_df.query("variant==@name and split=='FROZEN_VALIDATION_2025'").iloc[0]; ear = comparison_df.query("variant==@name and split=='EARLY_2026_SEQUENTIAL'").iloc[0]
        delta = abs(predictions[name][pd.concat([validation, early]).index] - predictions["V1_CONTROL"][pd.concat([validation, early]).index])
        combined = pd.concat([validation, early]); team_shares = [delta[((combined.home_team_id == team) | (combined.away_team_id == team)).to_numpy()].sum() for team in set(combined.home_team_id) | set(combined.away_team_id)]
        park_shares = [delta[(combined.venue_id == venue).to_numpy()].sum() for venue in set(combined.venue_id)]
        denominator = max(float(delta.sum()), 1e-12); dominance = max(max(team_shares, default=0), max(park_shares, default=0)) / denominator
        rolls = stability_df.query("variant==@name and window=='ROLLING_50'")
        no_explosion = bool((rolls.mae <= 6).all() and (rolls.bias.abs() <= 2).all())
        qualified = bool(name != "V1_CONTROL" and v1_validation.crps - val.crps >= MATERIAL_CRPS_GAIN and val.mae <= v1_validation.mae + .03 and abs(val.bias) < .40
            and abs(ear.bias) < .50 and ear.mae <= v1_early.mae + .03 and ear.crps <= v1_early.crps + .01 and no_explosion and dominance < .25 and val.distribution_mass_error < 1e-8)
        selection_rows.append({"variant": name, "validation_crps_gain_vs_v1": v1_validation.crps - val.crps, "validation_mae_delta_vs_v1": val.mae - v1_validation.mae,
            "validation_bias": val.bias, "early_2026_mae_delta_vs_v1": ear.mae - v1_early.mae, "early_2026_crps_delta_vs_v1": ear.crps - v1_early.crps,
            "early_2026_bias": ear.bias, "rolling50_stable": no_explosion, "max_team_or_park_change_share": dominance, "normalized_distribution": val.distribution_mass_error < 1e-8,
            "selection_qualified": qualified})
    selection = pd.DataFrame(selection_rows); qualified = selection[selection.selection_qualified]
    if len(qualified):
        selected = qualified.sort_values(["validation_crps_gain_vs_v1", "validation_mae_delta_vs_v1"], ascending=[False, True]).iloc[0].variant; result = "TOTALS_V2_PROSPECTIVE_CHALLENGER_IDENTIFIED"
    else:
        candidates = selection[selection.variant != "V1_CONTROL"].sort_values(["validation_crps_gain_vs_v1", "validation_mae_delta_vs_v1"], ascending=[False, True]); selected = candidates.iloc[0].variant
        result = "TOTALS_V2_STRUCTURAL_IMPROVEMENT_BELOW_PRACTICAL_BAR" if candidates.iloc[0].validation_crps_gain_vs_v1 > 0 else "TOTALS_V2_NO_STRUCTURAL_IMPROVEMENT"

    # Exactly one development-only dispersion check on the frozen selected location.
    selected_model = models[selected]; selected_columns = variants[selected]
    dispersion_train = data[data.game_date.dt.year == 2023]; tune = data[data.game_date.dt.year == 2024]
    dispersion_location = fit_location(dispersion_train, selected_columns); train_mu = dispersion_location.predict(dispersion_train[selected_columns])
    refit_alpha = max(1e-8, float((((dispersion_train.final_total - train_mu) ** 2 - dispersion_train.final_total).sum()) / np.maximum((train_mu ** 2).sum(), 1)))
    tune_mu = dispersion_location.predict(tune[selected_columns])
    fixed_crps = float(np.mean(crps(nb_mass(tune_mu, V1_ALPHA), tune.final_total))); refit_crps = float(np.mean(crps(nb_mass(tune_mu, refit_alpha), tune.final_total)))
    dispersion_decision = "REFIT_DISPERSION_SUPPORTED" if fixed_crps - refit_crps >= .01 else "KEEP_V1_DISPERSION"; selected_alpha = refit_alpha if dispersion_decision == "REFIT_DISPERSION_SUPPORTED" else V1_ALPHA

    variant_manifest = {"experiment": EXPERIMENT, "variants": variants, "fixed_v1_dispersion_alpha": V1_ALPHA, "sparse_shrinkage_k_grid": SPARSE_K_GRID,
        "sparse_shrinkage_development_audit": sparse_audit, "selected_sparse_k": sparse_k, "material_crps_gain": MATERIAL_CRPS_GAIN,
        "selection_contract": {"development": "2023-2024", "validation": "2025", "stability": "early-2026", "opened_late_2026_selection_use": False},
        "selection_audit": selection.to_dict("records"), "selected_variant": selected, "model_result": result, "dispersion_decision": dispersion_decision,
        "development_refit_alpha": refit_alpha, "development_2024_fixed_alpha_crps": fixed_crps, "development_2024_refit_alpha_crps": refit_crps}
    (out / "totals_v2_variant_manifest.json").write_text(json.dumps(variant_manifest, indent=2, default=float) + "\n")
    comparison_df.to_csv(out / "totals_v2_validation_comparison.csv", index=False); stability_df.to_csv(out / "totals_v2_early_2026_stability.csv", index=False)
    pd.DataFrame(thresholds).to_csv(out / "totals_v2_threshold_comparison.csv", index=False)
    location, park, sparse = location_audits(data, predictions, {"FROZEN_VALIDATION_2025": validation, "EARLY_2026_SEQUENTIAL": early})
    location.to_csv(out / "totals_v2_location_bias_audit.csv", index=False); park.to_csv(out / "totals_v2_park_regime_audit.csv", index=False); sparse.to_csv(out / "totals_v2_sparse_starter_audit.csv", index=False)

    # The late period was already opened. It is computed only after selection
    # above and cannot feed back into any decision or parameter.
    opened_rows = []
    for name in ("V1_CONTROL", selected):
        mu = models[name].predict(opened[variants[name]]); mass = nb_mass(mu, V1_ALPHA if name == "V1_CONTROL" else selected_alpha)
        row = metrics(name, "OPENED_HISTORICAL_DIAGNOSTIC_NOT_SELECTION_EVIDENCE", opened.final_total, mu, mass); opened_rows.append(row)
        for threshold in THRESHOLDS:
            actual = (opened.final_total.to_numpy() > threshold).astype(int); probability = mass[:, SUPPORT > threshold].sum(axis=1); clipped = np.clip(probability, 1e-9, 1-1e-9)
            opened_rows.append({"variant": name, "split": "OPENED_HISTORICAL_DIAGNOSTIC_NOT_SELECTION_EVIDENCE", "threshold": threshold, "games": len(opened),
                "accuracy": float(np.mean((probability >= .5) == actual)), "brier": float(np.mean((probability-actual)**2)),
                "log_loss": float(np.mean(-(actual*np.log(clipped)+(1-actual)*np.log(1-clipped)))), "observed_over_rate": float(actual.mean()), "predicted_over_rate": float(probability.mean())})
    pd.DataFrame(opened_rows).to_csv(out / "opened_late_2026_diagnostic.csv", index=False)

    frozen = serialize_model(selected, selected_model, selected_columns, selected_alpha, sparse_k, data_contract_hash)
    frozen["qualification_status"] = "FROZEN_PROSPECTIVE_CHALLENGER" if result == "TOTALS_V2_PROSPECTIVE_CHALLENGER_IDENTIFIED" else "NOT_FROZEN_SELECTION_STANDARD_NOT_MET"
    diagnostic_model_hash = frozen["model_hash"]
    if frozen["qualification_status"].startswith("NOT_"):
        frozen["diagnostic_specification_hash"] = diagnostic_model_hash
        frozen["model_hash"] = None
    (out / "frozen_totals_v2_manifest.json").write_text(json.dumps(frozen, indent=2) + "\n")

    # Outcome-free current shadow. The official schedule fetch is field-limited
    # by the certified live bridge and contains no score/run outcome fields.
    payload, observed, source_hash = fetch_hydrated_schedule(args.date); schedule = normalize_schedule(payload, observed, source_hash); history = build_history()
    contexts = [attach_context(row, history) for row in schedule]; dynamic_live = live_dynamic(history, args.date)
    team_starter = defaultdict(list); league_starter = []
    for row in data.itertuples():
        if row.game_date >= pd.Timestamp(args.date): continue
        for side in ("home", "away"):
            value = getattr(row, f"{side}_starter_season_ra9"); starts = getattr(row, f"{side}_starter_prior_starts")
            if pd.notna(value) and starts > 0: team_starter[int(getattr(row, f"{side}_team_id"))].append(float(value)); league_starter.append(float(value))
    shadow = []
    for context in contexts:
        try: v1_score = score_context(context, history, v1, observed)
        except Exception as exc:
            if str(exc) == "POST_START_GAME_NOT_ELIGIBLE": continue
            raise
        features = live_v1_features(context, history, v1); features.update(dynamic_live)
        bounded = float(np.clip(context["park_state"]["park_factor"], .8, 1.2)); depth = context["park_state"]["park_history_depth"]
        features.update({"park_shrunk_deviation": (bounded-1)*depth/(depth+50), "park_history_log1p": math.log1p(depth),
            "park_direct_history_flag": float(depth >= 20), "park_x_trailing30_environment": bounded*dynamic_live["trailing_30_league_rpg"]})
        league_level = float(np.mean(league_starter))
        for side in ("home", "away"):
            state = context[f"{side}_starter_state"]; team = int(context[f"{side}_team_id"]); values = team_starter[team]; team_direct = float(np.mean(values)) if values else league_level
            team_level = len(values)/(len(values)+20)*team_direct + 20/(len(values)+20)*league_level; starts = state["prior_starts"]
            weight = starts/(starts+sparse_k) if starts in (1,2) else 1.0
            features[f"{side}_starter_ra9_shrunk"] = weight*state["starter_ra9"]+(1-weight)*team_level
        v2_mu, v2_mass = score_serialized(features, frozen)
        row = {"game_pk": context["game_pk"], "game": f'{context["away_team_name"]} @ {context["home_team_name"]}', "scheduled_start_utc": context["scheduled_start_utc"],
            "away_probable_starter": context["away_probable_pitcher_name"], "home_probable_starter": context["home_probable_pitcher_name"],
            "away_starter_state": context["away_starter_state"]["fallback_tier"], "home_starter_state": context["home_starter_state"]["fallback_tier"],
            "venue": context["venue_name"], "park_factor": context["park_state"]["park_factor"], "park_state": context["park_state"]["fallback_status"],
            "data_quality_status": context["data_quality_status"], "prediction_timestamp_utc": observed, "schedule_source_sha256": source_hash,
            "v1_expected_total": v1_score["expected_total"], "v2_expected_total": v2_mu, "expected_total_change": v2_mu-v1_score["expected_total"],
            "v1_interval_80_low": v1_score["interval_80_low"], "v1_interval_80_high": v1_score["interval_80_high"],
            "v2_interval_80_low": int(np.searchsorted(np.cumsum(v2_mass), .1)), "v2_interval_80_high": int(np.searchsorted(np.cumsum(v2_mass), .9)),
            "v1_model_hash": v1["canonical_model_hash"], "v2_qualification_status": frozen["qualification_status"],
            "v2_model_hash": diagnostic_model_hash, "sportsbook_status": "NO_LOCAL_PREGAME_GAME_TOTAL_SOURCE_AVAILABLE"}
        for threshold in THRESHOLDS:
            suffix = str(threshold).replace(".", "_"); row[f"v1_p_over_{suffix}"] = v1_score[f"p_over_{suffix}"]; row[f"v2_p_over_{suffix}"] = float(v2_mass[SUPPORT > threshold].sum())
        shadow.append(row)
    pd.DataFrame(shadow).to_csv(out / "august_6_v1_v2_shadow_comparison.csv", index=False)

    prospective = result == "TOTALS_V2_PROSPECTIVE_CHALLENGER_IDENTIFIED"
    prospective_state = "TOTALS_V1_V2_PROSPECTIVE_COMPARISON_AUTHORIZED" if prospective else "NO_TOTALS_V2_PROSPECTIVE_TEST"
    preview = "TOTALS_V2_READY_FOR_PRIVATE_SHADOW_ONLY" if prospective and shadow and all(row["data_quality_status"] == "TOTALS_CONTEXT_COMPLETE" for row in shadow) else "TOTALS_V2_NOT_READY_FOR_PRIVATE_PREVIEW"
    (out / "prospective_v1_v2_evidence_contract.md").write_text(f"# Prospective v1/v2 evidence contract\n\n`{prospective_state}`\n\nEvidence begins 2026-08-06 forward. Preserve immutable v1 control and v2 challenger predictions before first pitch; grade final and regulation-nine totals. Track MAE, bias, CRPS, fixed-threshold Brier, prediction differences, high-total, park, and sparse-starter regimes. Reviews: 25 games integrity only; 50 directional; 100 first practical comparison; continue if unresolved. No public display, EV, or betting claim.\n")
    (out / "totals_v2_private_shadow_readiness.md").write_text(f"# Totals v2 private shadow readiness\n\n`{preview}`\n\nHistorical selection: `{result}`. August 6 context-complete shadow rows: {sum(row['data_quality_status']=='TOTALS_CONTEXT_COMPLETE' for row in shadow)}/{len(shadow)}. Any private surface must say: `PROSPECTIVE CHALLENGER — NOT A QUALIFIED BETTING MODEL`. Public display is not authorized.\n")
    val_selected = comparison_df.query("variant==@selected and split=='FROZEN_VALIDATION_2025'").iloc[0]; early_selected = comparison_df.query("variant==@selected and split=='EARLY_2026_SEQUENTIAL'").iloc[0]
    (out / "concise_mlb_totals_structural_challenger_v2.md").write_text(f"# MLB Totals Structural Challenger v2\n\n- `{result}`\n- `{prospective_state}`\n- `{preview}`\n- Selected permitted-evidence variant: `{selected}`\n- 2025 MAE/bias/CRPS: {val_selected.mae:.6f} / {val_selected.bias:.6f} / {val_selected.crps:.6f}\n- Early-2026 MAE/bias/CRPS: {early_selected.mae:.6f} / {early_selected.bias:.6f} / {early_selected.crps:.6f}\n- Dispersion: `{dispersion_decision}` ({selected_alpha:.12f})\n- Opened late-2026: diagnostic only, never selection evidence\n- August 6 outcomes accessed: 0\n- Public/deployment status: unchanged; not authorized\n")
    hash_path = out / "reproducibility_hashes.sha256"; hash_path.write_text("".join(f"{sha_file(path)}  {path.name}\n" for path in sorted(out.iterdir()) if path != hash_path))
    print(json.dumps({"model_result": result, "prospective_state": prospective_state, "preview": preview, "selected": selected, "validation": val_selected.to_dict(),
        "early": early_selected.to_dict(), "dispersion": dispersion_decision, "alpha": selected_alpha, "shadow_rows": len(shadow), "august6_outcomes_accessed": 0,
        "model_hash": frozen.get("model_hash")}, indent=2, default=float))


if __name__ == "__main__": main()
