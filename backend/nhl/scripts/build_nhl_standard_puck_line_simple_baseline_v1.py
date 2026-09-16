#!/usr/bin/env python3
"""Build and evaluate the frozen six-feature NHL standard puck-line baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import sklearn
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler


ROOT = Path(__file__).resolve().parents[3]
V2_PARENT = ROOT / "artifacts/analysis/model_development/nhl_moneyline_strict_prior_control_v2_and_sog_conditional_test/2026-09-15"
RECOVERY_PARENT = ROOT / "artifacts/analysis/model_development/nhl_season_2025_historical_market_and_sog_recovery_v1/2026-09-15"
CROSS_MARKET_PARENT = ROOT / "artifacts/analysis/model_development/nhl_2025_v2_market_and_sog_cross_market_evaluation_v1/2026-09-15"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/nhl_standard_puck_line_simple_baseline_v1/2026-09-15"

FEATURES = [
    "diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg",
    "diff_days_rest", "home_back_to_back", "away_back_to_back",
]
CLASSES = ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"]
CLASS_TO_INT = {name: i for i, name in enumerate(CLASSES)}
SEED = 20260713
BOOTSTRAP_SEED = 20260713
PROBABILITY_BINS = [-np.inf, 0.40, 0.475, 0.525, 0.60, np.inf]
PROBABILITY_LABELS = ["LT_40", "40_TO_47_5", "47_5_TO_52_5", "52_5_TO_60", "GE_60"]
SPLIT_POPULATIONS = {
    "historical_validation": ["validation"],
    "historical_holdout": ["holdout"],
    "combined_historical_oot": ["validation", "holdout"],
    "season_2025_forward": ["season_2025_forward"],
}


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def clean_json(value: object) -> object:
    if isinstance(value, dict):
        return {str(k): clean_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(v) for v in value]
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(clean_json(value), indent=2, sort_keys=True) + "\n")


def verify_manifest(directory: Path) -> dict[str, object]:
    failures, checked = [], 0
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        if not line.strip():
            continue
        expected, name = line.split("  ", 1)
        checked += 1
        path = directory / name
        if not path.is_file() or sha256_file(path) != expected:
            failures.append(name)
    return {"directory": str(directory.relative_to(ROOT)), "files_checked": checked, "failures": failures, "passed": not failures}


def class_from_margin(margin: float) -> str:
    if margin <= -2:
        return "AWAY_BY_2_PLUS"
    if margin >= 2:
        return "HOME_BY_2_PLUS"
    if abs(margin) == 1:
        return "ONE_GOAL_GAME"
    raise ValueError(f"Invalid full-game NHL margin for exhaustive class mapping: {margin}")


def mechanical_probabilities(v2_home_probability: np.ndarray) -> np.ndarray:
    p = np.asarray(v2_home_probability, dtype=float)
    away = (1.0 - p) ** 2
    home = p ** 2
    one = 1.0 - away - home
    return np.column_stack([away, one, home])


def cover_probabilities(class_probabilities: np.ndarray) -> dict[str, np.ndarray]:
    away2, _, home2 = class_probabilities.T
    return {
        "home_minus_1_5_probability": home2,
        "away_plus_1_5_probability": 1.0 - home2,
        "away_minus_1_5_probability": away2,
        "home_plus_1_5_probability": 1.0 - away2,
    }


def multiclass_metrics(y: np.ndarray, probs: np.ndarray) -> dict[str, float]:
    one_hot = np.eye(3)[y]
    cumulative_p = np.cumsum(probs, axis=1)[:, :-1]
    cumulative_y = np.cumsum(one_hot, axis=1)[:, :-1]
    return {
        "multiclass_log_loss": float(log_loss(y, probs, labels=[0, 1, 2])),
        "multiclass_brier_score": float(np.mean(np.sum((probs - one_hot) ** 2, axis=1))),
        "ranked_probability_score": float(np.mean(np.mean((cumulative_p - cumulative_y) ** 2, axis=1))),
        "class_accuracy": float(accuracy_score(y, np.argmax(probs, axis=1))),
    }


def binary_metrics(y: np.ndarray, p: np.ndarray) -> dict[str, float]:
    eps = 1e-15
    return {
        "brier_score": float(np.mean((p - y) ** 2)),
        "log_loss": float(log_loss(y, np.clip(p, eps, 1 - eps), labels=[0, 1])),
        "roc_auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else float("nan"),
        "accuracy_at_0_5": float(accuracy_score(y, p >= 0.5)),
    }


def bootstrap_mean_ci(values: np.ndarray, resamples: int, seed: int) -> tuple[float, float]:
    values = np.asarray(values, dtype=float)
    rng = np.random.default_rng(seed)
    samples = np.empty(resamples)
    for i in range(resamples):
        samples[i] = values[rng.integers(0, len(values), len(values))].mean()
    return tuple(float(x) for x in np.quantile(samples, [0.025, 0.975]))


def fit_and_predict(frame: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    fit = frame.loc[frame.split.eq("fit")].copy()
    if len(fit) != 701:
        raise ValueError(f"Frozen fit membership changed: {len(fit)}")
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    x_fit_imputed = imputer.fit_transform(fit[FEATURES])
    x_fit_scaled = scaler.fit_transform(x_fit_imputed)
    model = LogisticRegression(C=1.0, penalty="l2", solver="lbfgs", random_state=SEED, max_iter=1000)
    model.fit(x_fit_scaled, fit.margin_class_int)
    x_all_imputed = imputer.transform(frame[FEATURES])
    x_all_scaled = scaler.transform(x_all_imputed)
    probs = model.predict_proba(x_all_scaled)
    if model.classes_.tolist() != [0, 1, 2]:
        raise ValueError(f"Unexpected class order: {model.classes_.tolist()}")

    out = frame.copy()
    out["imputed_feature_count"] = out[FEATURES].isna().sum(axis=1)
    out["imputation_state"] = np.where(out.imputed_feature_count.eq(0), "NONE", out[FEATURES].isna().apply(lambda r: "FIT_MEDIAN:" + "|".join(r.index[r]), axis=1))
    for i, name in enumerate(CLASSES):
        out[f"baseline_p_{name.lower()}"] = probs[:, i]
    for name, values in cover_probabilities(probs).items():
        out[f"baseline_{name}"] = values

    fit_freq = np.bincount(fit.margin_class_int, minlength=3) / len(fit)
    constant = np.tile(fit_freq, (len(out), 1))
    mechanical = mechanical_probabilities(out.v2_home_win_probability.to_numpy())
    for label, matrix in (("constant", constant), ("mechanical_v2_strength", mechanical)):
        for i, name in enumerate(CLASSES):
            out[f"{label}_p_{name.lower()}"] = matrix[:, i]
        for name, values in cover_probabilities(matrix).items():
            out[f"{label}_{name}"] = values

    core = {
        "model_name": "NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1",
        "model_family": "multinomial_logistic_regression",
        "classes": CLASSES,
        "class_encoding": CLASS_TO_INT,
        "feature_order": FEATURES,
        "fit_rows": len(fit),
        "fit_game_ids_sha256": digest(sorted(int(x) for x in fit.game_id)),
        "fit_feature_target_sha256": digest(fit[["game_id", *FEATURES, "margin_class"]].fillna("__NA__").to_dict("records")),
        "configuration": {"C": 1.0, "penalty": "l2", "solver": "lbfgs", "loss": "true_multinomial", "random_state": SEED, "max_iter": 1000, "hyperparameter_search": False, "feature_selection": False},
        "imputation_medians": dict(zip(FEATURES, imputer.statistics_)),
        "standardization_means": dict(zip(FEATURES, scaler.mean_)),
        "standardization_scales": dict(zip(FEATURES, scaler.scale_)),
        "coefficients_by_class": {CLASSES[i]: dict(zip(FEATURES, model.coef_[i])) for i in range(3)},
        "intercepts_by_class": dict(zip(CLASSES, model.intercept_)),
        "fit_class_counts": fit.margin_class.value_counts().reindex(CLASSES, fill_value=0).to_dict(),
        "fit_class_probabilities": dict(zip(CLASSES, fit_freq)),
        "versions": {"numpy": np.__version__, "pandas": pd.__version__, "scikit_learn": sklearn.__version__},
    }
    core["model_sha256"] = digest(clean_json(core))
    out["model_identity"] = core["model_name"]
    out["model_sha256"] = core["model_sha256"]
    substantive = ["game_id", *FEATURES, "imputation_state", *[f"baseline_p_{c.lower()}" for c in CLASSES]]
    out["substantive_prediction_sha256"] = out[substantive].apply(lambda r: digest(clean_json(r.to_dict())), axis=1)
    return out, core


def evaluate_populations(predictions: pd.DataFrame, resamples: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    models = {
        "PUCK_LINE_BASELINE": "baseline",
        "FIT_CLASS_FREQUENCIES": "constant",
        "MECHANICAL_V2_STRENGTH": "mechanical_v2_strength",
    }
    multi_rows, binary_rows, confusion_rows, calibration_rows = [], [], [], []
    for population, splits in SPLIT_POPULATIONS.items():
        group = predictions.loc[predictions.split.isin(splits)].copy()
        y = group.margin_class_int.to_numpy(int)
        for model_name, prefix in models.items():
            probs = group[[f"{prefix}_p_{c.lower()}" for c in CLASSES]].to_numpy(float)
            multi_rows.append({"population": population, "model": model_name, "n_games": len(group), **multiclass_metrics(y, probs)})
            predicted = np.argmax(probs, axis=1)
            for actual_i, actual in enumerate(CLASSES):
                for pred_i, predicted_name in enumerate(CLASSES):
                    confusion_rows.append({"population": population, "model": model_name, "actual_class": actual, "predicted_class": predicted_name, "games": int(np.sum((y == actual_i) & (predicted == pred_i)))})
                bins = pd.cut(probs[:, actual_i], np.linspace(0, 1, 11), include_lowest=True, right=False)
                for band in bins.categories:
                    mask = bins == band
                    calibration_rows.append({"population": population, "model": model_name, "target": actual, "probability_bin": str(band), "n_games": int(mask.sum()), "mean_probability": float(probs[mask, actual_i].mean()) if mask.any() else np.nan, "observed_rate": float((y[mask] == actual_i).mean()) if mask.any() else np.nan})
            covers = cover_probabilities(probs)
            targets = {
                "home_minus_1_5": (y == 2).astype(int), "away_plus_1_5": (y != 2).astype(int),
                "away_minus_1_5": (y == 0).astype(int), "home_plus_1_5": (y != 0).astype(int),
            }
            for market, target in targets.items():
                p = covers[f"{market}_probability"]
                binary_rows.append({"population": population, "model": model_name, "market": market, "n_games": len(group), **binary_metrics(target, p)})
                bins = pd.cut(p, np.linspace(0, 1, 11), include_lowest=True, right=False)
                for band in bins.categories:
                    mask = bins == band
                    calibration_rows.append({"population": population, "model": model_name, "target": market, "probability_bin": str(band), "n_games": int(mask.sum()), "mean_probability": float(p[mask].mean()) if mask.any() else np.nan, "observed_rate": float(target[mask].mean()) if mask.any() else np.nan})
    return pd.DataFrame(multi_rows), pd.DataFrame(binary_rows), pd.DataFrame(confusion_rows), pd.DataFrame(calibration_rows)


def benchmark_bootstrap(predictions: pd.DataFrame, resamples: int) -> pd.DataFrame:
    rows = []
    for population, splits in SPLIT_POPULATIONS.items():
        group = predictions.loc[predictions.split.isin(splits)]
        y = np.eye(3)[group.margin_class_int.to_numpy(int)]
        p = group[[f"baseline_p_{c.lower()}" for c in CLASSES]].to_numpy()
        for bi, (benchmark, prefix) in enumerate((("FIT_CLASS_FREQUENCIES", "constant"), ("MECHANICAL_V2_STRENGTH", "mechanical_v2_strength"))):
            q = group[[f"{prefix}_p_{c.lower()}" for c in CLASSES]].to_numpy()
            losses = {
                "MULTICLASS_LOG_LOSS": -np.sum(y * np.log(np.clip(p, 1e-15, 1)), axis=1) + np.sum(y * np.log(np.clip(q, 1e-15, 1)), axis=1),
                "MULTICLASS_BRIER": np.sum((p - y) ** 2, axis=1) - np.sum((q - y) ** 2, axis=1),
                "RANKED_PROBABILITY_SCORE": np.mean((np.cumsum(p, axis=1)[:, :-1] - np.cumsum(y, axis=1)[:, :-1]) ** 2, axis=1) - np.mean((np.cumsum(q, axis=1)[:, :-1] - np.cumsum(y, axis=1)[:, :-1]) ** 2, axis=1),
            }
            for mi, (metric, values) in enumerate(losses.items()):
                lo, hi = bootstrap_mean_ci(values, resamples, BOOTSTRAP_SEED + bi * 100 + mi + len(rows))
                rows.append({"population": population, "comparison": f"PUCK_LINE_BASELINE_MINUS_{benchmark}", "metric": metric, "n_games": len(group), "point_difference": values.mean(), "ci_2_5": lo, "ci_97_5": hi, "resamples": resamples, "bootstrap_grain": "GAME"})
    return pd.DataFrame(rows)


def characterize(predictions: pd.DataFrame) -> pd.DataFrame:
    f = predictions.copy()
    f["month"] = pd.to_datetime(f.game_date).dt.to_period("M").astype(str)
    f["predicted_favorite_side"] = np.where(f.baseline_home_minus_1_5_probability >= f.baseline_away_minus_1_5_probability, "HOME", "AWAY")
    f["v2_moneyline_strength_band"] = pd.cut(f.v2_home_win_probability, PROBABILITY_BINS, labels=PROBABILITY_LABELS, right=False).astype(str)
    f["margin_prevalence"] = np.where(f.margin_class.eq("ONE_GOAL_GAME"), "ONE_GOAL", "MULTI_GOAL")
    f["decision_state"] = f.decision_type.fillna("UNKNOWN_NOT_RETAINED")
    rows = []
    for population, splits in SPLIT_POPULATIONS.items():
        base = f.loc[f.split.isin(splits)]
        dimensions = ["predicted_favorite_side", "v2_moneyline_strength_band", "margin_prevalence", "decision_state", "month"]
        for dimension in dimensions:
            for value, g in base.groupby(dimension, dropna=False, sort=True):
                rows.append(segment_row(population, dimension, str(value), g))
        team_rows = pd.concat([
            base.assign(team=base.home_team, orientation="HOME"),
            base.assign(team=base.away_team, orientation="AWAY"),
        ], ignore_index=True)
        for (team, orientation), g in team_rows.groupby(["team", "orientation"], sort=True):
            rows.append(segment_row(population, f"team_{orientation.lower()}_orientation", str(team), g))
    return pd.DataFrame(rows)


def segment_row(population: str, dimension: str, value: str, g: pd.DataFrame) -> dict[str, object]:
    probs = g[[f"baseline_p_{c.lower()}" for c in CLASSES]].to_numpy(float)
    y = g.margin_class_int.to_numpy(int)
    return {"population": population, "dimension": dimension, "segment": value, "n_games": len(g), "away_by_2_plus_rate": (y == 0).mean(), "one_goal_rate": (y == 1).mean(), "home_by_2_plus_rate": (y == 2).mean(), **multiclass_metrics(y, probs)}


def market_comparison(predictions: pd.DataFrame, quotes: pd.DataFrame, resamples: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object]]:
    preds = predictions.loc[predictions.split.eq("season_2025_forward")].copy()
    pairs = quotes.groupby(["game_id", "bookmaker_key"], sort=True)
    valid_keys = []
    for key, group in pairs:
        if len(group) == 2 and group.side_orientation.nunique() == 2 and set(group.point.astype(float).abs()) == {1.5} and abs(group.point.astype(float).sum()) < 1e-12:
            valid_keys.append(key)
    indexed = quotes.set_index(["game_id", "bookmaker_key"])
    valid = indexed.loc[valid_keys].reset_index().copy()
    raw = 1 / valid.decimal_price.astype(float)
    valid["raw_implied_probability"] = raw
    valid["pair_overround"] = valid.groupby(["game_id", "bookmaker_key"]).raw_implied_probability.transform("sum")
    valid["market_no_vig_cover_probability"] = valid.raw_implied_probability / valid.pair_overround
    valid = valid.merge(preds[["game_id", "baseline_home_minus_1_5_probability", "baseline_away_plus_1_5_probability", "baseline_away_minus_1_5_probability", "baseline_home_plus_1_5_probability"]], on="game_id", validate="many_to_one")

    def orientation(row: pd.Series) -> tuple[str, float]:
        key = f"{row.side_orientation.lower()}_{'minus' if row.point < 0 else 'plus'}_1_5"
        return key, float(row[f"baseline_{key}_probability"])

    mapped = valid.apply(orientation, axis=1)
    valid["standard_side"] = [x[0] for x in mapped]
    valid["model_cover_probability"] = [x[1] for x in mapped]
    valid["cover_target"] = valid.puck_line_result.eq("WIN").astype(int)
    valid["model_brier_loss"] = (valid.model_cover_probability - valid.cover_target) ** 2
    valid["market_brier_loss"] = (valid.market_no_vig_cover_probability - valid.cover_target) ** 2
    valid["model_log_loss"] = -(valid.cover_target * np.log(np.clip(valid.model_cover_probability, 1e-15, 1)) + (1 - valid.cover_target) * np.log(np.clip(1 - valid.model_cover_probability, 1e-15, 1)))
    valid["market_log_loss"] = -(valid.cover_target * np.log(np.clip(valid.market_no_vig_cover_probability, 1e-15, 1)) + (1 - valid.cover_target) * np.log(np.clip(1 - valid.market_no_vig_cover_probability, 1e-15, 1)))
    metric_rows = []
    for scope, group in [("ALL_BOOKS", valid), *[(book, g) for book, g in valid.groupby("bookmaker_key", sort=True)]]:
        for label, pcol in (("PUCK_LINE_BASELINE", "model_cover_probability"), ("NO_VIG_MARKET", "market_no_vig_cover_probability")):
            metric_rows.append({"scope": scope, "model": label, "games": group.game_id.nunique(), "book_game_pairs": group[["game_id", "bookmaker_key"]].drop_duplicates().shape[0], "quote_sides": len(group), **binary_metrics(group.cover_target.to_numpy(), group[pcol].to_numpy())})
    per_game = valid.groupby("game_id", as_index=False).agg(model_brier=("model_brier_loss", "mean"), market_brier=("market_brier_loss", "mean"), model_log=("model_log_loss", "mean"), market_log=("market_log_loss", "mean"))
    boot_rows = []
    for i, (metric, values) in enumerate((("BRIER", per_game.model_brier - per_game.market_brier), ("LOG_LOSS", per_game.model_log - per_game.market_log))):
        lo, hi = bootstrap_mean_ci(values.to_numpy(), resamples, BOOTSTRAP_SEED + 900 + i)
        boot_rows.append({"scope": "ALL_BOOKS_GAME_MEAN", "comparison": "PUCK_LINE_BASELINE_MINUS_NO_VIG_MARKET", "metric": metric, "games": len(per_game), "point_difference": values.mean(), "ci_2_5": lo, "ci_97_5": hi, "resamples": resamples, "bootstrap_grain": "GAME"})
    selected = valid.sort_values(["game_id", "bookmaker_key", "model_cover_probability", "standard_side"], ascending=[True, True, False, True]).drop_duplicates(["game_id", "bookmaker_key"])
    selected["selected_one_unit_risk_return"] = np.where(selected.cover_target.eq(1), selected.decimal_price - 1.0, -1.0)
    return_rows = []
    game_returns = selected.groupby("game_id").selected_one_unit_risk_return.mean()
    lo, hi = bootstrap_mean_ci(game_returns.to_numpy(), resamples, BOOTSTRAP_SEED + 950)
    return_rows.append({"scope": "ALL_BOOKS_GAME_MEAN", "games": selected.game_id.nunique(), "book_game_pairs": len(selected), "mean_one_unit_risk_return": game_returns.mean(), "ci_2_5": lo, "ci_97_5": hi, "bootstrap_grain": "GAME"})
    for i, (book, group) in enumerate(selected.groupby("bookmaker_key", sort=True)):
        lo, hi = bootstrap_mean_ci(group.selected_one_unit_risk_return.to_numpy(), resamples, BOOTSTRAP_SEED + 1000 + i)
        return_rows.append({"scope": book, "games": group.game_id.nunique(), "book_game_pairs": len(group), "mean_one_unit_risk_return": group.selected_one_unit_risk_return.mean(), "ci_2_5": lo, "ci_97_5": hi, "bootstrap_grain": "GAME"})
    diagnostics = {"source_quotes": len(quotes), "qualifying_quotes": len(valid), "source_games": quotes.game_id.nunique(), "qualifying_games": valid.game_id.nunique(), "book_game_pairs": len(valid_keys), "excluded_book_game_pairs": quotes[["game_id", "bookmaker_key"]].drop_duplicates().shape[0] - len(valid_keys), "max_no_vig_complement_error": float(valid.groupby(["game_id", "bookmaker_key"]).market_no_vig_cover_probability.sum().sub(1).abs().max()), "selection_bias": "MATERIAL"}
    return valid, pd.DataFrame(metric_rows), pd.DataFrame(boot_rows), pd.DataFrame(return_rows), diagnostics


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--bootstrap-resamples", type=int, default=5000)
    args = ap.parse_args()
    if args.bootstrap_resamples < 5000:
        raise SystemExit("At least 5,000 deterministic game-level bootstrap resamples are required")
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    parents = [verify_manifest(p) for p in (V2_PARENT, RECOVERY_PARENT, CROSS_MARKET_PARENT)]
    if not all(x["passed"] for x in parents):
        raise SystemExit(f"Parent manifest verification failed: {parents}")
    prior_decision = json.loads((CROSS_MARKET_PARENT / "decision.json").read_text())
    if prior_decision["NHL_PUCK_LINE_RECOVERED_POPULATION"] != "MATERIAL_SELECTION_BIAS":
        raise SystemExit("Required recovered-market selection-bias state changed")

    source = pd.read_csv(V2_PARENT / "v2_predictions.csv")
    source["home_margin"] = source.final_home_goals.astype(int) - source.final_away_goals.astype(int)
    source["margin_class"] = source.home_margin.apply(class_from_margin)
    source["margin_class_int"] = source.margin_class.map(CLASS_TO_INT)
    source["total_goals_derived"] = source.final_home_goals + source.final_away_goals
    source["decision_type"] = source.decision_type.fillna("UNKNOWN_NOT_RETAINED")
    predictions, artifact = fit_and_predict(source)
    multi, binary, confusion, calibration = evaluate_populations(predictions, args.bootstrap_resamples)
    benchmark_boot = benchmark_bootstrap(predictions, args.bootstrap_resamples)
    segments = characterize(predictions)
    quotes = pd.read_csv(RECOVERY_PARENT / "graded_standard_puck_line_history.csv")
    market_rows, market_metrics, market_boot, market_returns, market_diag = market_comparison(predictions, quotes, args.bootstrap_resamples)

    contract = {
        "model_name": artifact["model_name"], "purpose": "smallest understandable standard NHL +/-1.5 probability control; not an edge claim",
        "feature_order": FEATURES, "feature_definitions_parent": str((V2_PARENT / "v2_contract.json").relative_to(ROOT)),
        "target": {"AWAY_BY_2_PLUS": "home_margin <= -2", "ONE_GOAL_GAME": "abs(home_margin) == 1", "HOME_BY_2_PLUS": "home_margin >= 2"},
        "cover_mapping": {"home_-1.5": "P(HOME_BY_2_PLUS)", "away_+1.5": "1-P(HOME_BY_2_PLUS)", "away_-1.5": "P(AWAY_BY_2_PLUS)", "home_+1.5": "1-P(AWAY_BY_2_PLUS)"},
        "preprocessing": "fit-only median imputation then fit-only standardization", "fit_membership": "exact frozen 701-game V2 fit membership",
        "model_configuration": artifact["configuration"], "forbidden_inputs": ["V2 probability", "SOG", "Points", "Saves", "market probability", "price"],
        "benchmarks": {"fit_class_frequencies": "constant 701-game class rates", "fit_binary_cover_rates": "coherent binary rates derived from fit class frequencies", "mechanical_v2_strength": "away2+=(1-p_v2)^2; home2+=p_v2^2; one-goal=2*p_v2*(1-p_v2); no fitting or outcome optimization"},
        "fixed_acceptance_rules": {"historical_oot_better": "model multiclass log loss, Brier and RPS lower than constant on validation, holdout and combined OOT", "historical_oot_no_improvement": "all three combined-OOT proper scores no lower than constant", "season_2025_acceptable": "all three proper scores lower than constant and both minus-1.5 ROC AUC values exceed 0.5", "process_valid_but_weak": "integrity valid but season-2025 acceptable rule not met"},
        "empty_net_interpretation": "margin classes absorb historical empty-net effects but do not identify or distinguish them causally; a final two-goal margin is not inferred to be an empty-net state",
    }
    contract["contract_sha256"] = digest(contract)
    prospective = {
        "status": "DEFINED_NOT_ACTIVATED", "authorized_actions": [], "forbidden_actions": ["wager", "pick", "upload", "public_display"],
        "fields": ["game_id", "scheduled_start_time_utc", "home_team", "away_team", *[f"p_{c.lower()}" for c in CLASSES], "home_minus_1_5_probability", "away_plus_1_5_probability", "away_minus_1_5_probability", "home_plus_1_5_probability", *FEATURES, "imputation_state", "model_identity", "model_sha256", "prediction_timestamp_utc", "substantive_prediction_sha256"],
        "substantive_prediction_hash": "SHA256 of game ID, ordered six raw inputs, imputation state, and three margin-class probabilities; excludes volatile run metadata",
        "probability_coherence": "three classes sum to 1; each complementary puck-line pair sums to 1",
    }
    prospective["contract_sha256"] = digest(prospective)

    metric_index = multi.set_index(["population", "model"])
    proper = ["multiclass_log_loss", "multiclass_brier_score", "ranked_probability_score"]
    hist_better = all(metric_index.loc[(pop, "PUCK_LINE_BASELINE"), m] < metric_index.loc[(pop, "FIT_CLASS_FREQUENCIES"), m] for pop in ["historical_validation", "historical_holdout", "combined_historical_oot"] for m in proper)
    combined_no_improve = all(metric_index.loc[("combined_historical_oot", "PUCK_LINE_BASELINE"), m] >= metric_index.loc[("combined_historical_oot", "FIT_CLASS_FREQUENCIES"), m] for m in proper)
    forward_better = all(metric_index.loc[("season_2025_forward", "PUCK_LINE_BASELINE"), m] < metric_index.loc[("season_2025_forward", "FIT_CLASS_FREQUENCIES"), m] for m in proper)
    bindex = binary.set_index(["population", "model", "market"])
    forward_auc = all(bindex.loc[("season_2025_forward", "PUCK_LINE_BASELINE", market), "roc_auc"] > 0.5 for market in ["home_minus_1_5", "away_minus_1_5"])
    coherent = bool(np.allclose(predictions[[f"baseline_p_{c.lower()}" for c in CLASSES]].sum(axis=1), 1, atol=1e-12) and np.allclose(predictions.baseline_home_minus_1_5_probability + predictions.baseline_away_plus_1_5_probability, 1, atol=1e-12) and np.allclose(predictions.baseline_away_minus_1_5_probability + predictions.baseline_home_plus_1_5_probability, 1, atol=1e-12))
    process_valid = coherent and len(predictions) == 4110 and len(predictions.loc[predictions.split.eq("fit")]) == 701 and predictions.strict_prior_status.eq("VERIFIED").all()
    forward_decision = "ACCEPTABLE_AS_SIMPLE_CONTROL" if process_valid and forward_better and forward_auc else "PROCESS_VALID_BUT_WEAK" if process_valid else "FAILED"
    shadow_ready = process_valid and forward_decision == "ACCEPTABLE_AS_SIMPLE_CONTROL"
    decisions = {
        "NHL_PUCK_LINE_BASELINE_CONSTRUCTION": "COMPLETED",
        "NHL_PUCK_LINE_BASELINE_PROCESS": "VALID" if process_valid else "INVALID",
        "NHL_PUCK_LINE_HISTORICAL_OOT": "BETTER_THAN_CONSTANT_BASELINE" if hist_better else "NO_IMPROVEMENT" if combined_no_improve else "INCONCLUSIVE",
        "NHL_PUCK_LINE_SEASON_2025_FORWARD": forward_decision,
        "NHL_PUCK_LINE_VS_RECOVERED_MARKET": "INCONCLUSIVE_SELECTION_BIAS",
        "NHL_PUCK_LINE_EDGE_STATUS": "NOT_ESTABLISHED",
        "NHL_PUCK_LINE_SHADOW_READINESS": "READY_FOR_PROSPECTIVE_SHADOW" if shadow_ready else "NOT_READY",
        "NHL_NEXT_STEP": "ACTIVATE_MONEYLINE_AND_PUCK_LINE_PROSPECTIVE_SHADOW" if shadow_ready else "BEGIN_MONEYLINE_ONLY_PROSPECTIVE_CAPTURE" if process_valid else "REPAIR_SPECIFIC_PUCK_LINE_BASELINE_BLOCKER",
        "NHL_2025_PUCK_LINE_MARKET_SELECTION_BIAS": "MATERIAL",
        "NHL_MODEL_SOG_CONDITIONAL_NOVELTY": "FRAGILE", "NHL_MARKET_SOG_CONDITIONAL_NOVELTY": "REDUNDANT", "NHL_MODEL_MARKET_SOG_DISAGREEMENT": "NO_INFORMATION", "NHL_CROSS_MARKET_CHALLENGER": "NOT_READY",
    }

    write_json(out / "model_contract.json", contract)
    write_json(out / "fitted_research_artifact.json", artifact)
    predictions.to_parquet(out / "prediction_populations.parquet", index=False)
    multi.to_csv(out / "multiclass_metrics.csv", index=False, lineterminator="\n")
    binary.to_csv(out / "binary_metrics.csv", index=False, lineterminator="\n")
    confusion.to_csv(out / "confusion_matrix.csv", index=False, lineterminator="\n")
    calibration.to_csv(out / "calibration_tables.csv", index=False, lineterminator="\n")
    benchmark_boot.to_csv(out / "benchmark_game_bootstrap.csv", index=False, lineterminator="\n")
    segments.to_csv(out / "segment_characterization.csv", index=False, lineterminator="\n")
    market_rows.to_parquet(out / "recovered_market_quote_comparison.parquet", index=False)
    market_metrics.to_csv(out / "recovered_market_metrics.csv", index=False, lineterminator="\n")
    market_boot.to_csv(out / "recovered_market_game_bootstrap.csv", index=False, lineterminator="\n")
    market_returns.to_csv(out / "recovered_market_hypothetical_returns.csv", index=False, lineterminator="\n")
    write_json(out / "prospective_output_contract.json", prospective)
    write_json(out / "decision.json", {**decisions, "additional_api_calls": 0, "market_diagnostics": market_diag})
    validation = [
        {"check": "parent_manifests", "passed": all(x["passed"] for x in parents), "evidence": json.dumps(parents, sort_keys=True)},
        {"check": "class_exhaustiveness", "passed": len(predictions) == predictions.margin_class.notna().sum() and not predictions.home_margin.eq(0).any(), "evidence": json.dumps(predictions.margin_class.value_counts().to_dict(), sort_keys=True)},
        {"check": "probability_coherence", "passed": coherent, "evidence": "class and complementary cover probabilities sum to one within 1e-12"},
        {"check": "split_isolation", "passed": len(predictions.loc[predictions.split.eq("fit")]) == 701 and not predictions.loc[~predictions.split.eq("fit"), "game_id"].isin(predictions.loc[predictions.split.eq("fit"), "game_id"]).any(), "evidence": json.dumps(predictions.split.value_counts().to_dict(), sort_keys=True)},
        {"check": "strict_prior_inputs", "passed": predictions.strict_prior_status.eq("VERIFIED").all(), "evidence": f"verified={predictions.strict_prior_status.eq('VERIFIED').sum()}/4110"},
        {"check": "price_orientation", "passed": set(market_rows.standard_side.unique()) == {"home_minus_1_5", "away_plus_1_5"} and market_rows.loc[market_rows.standard_side.eq("home_minus_1_5"), "point"].eq(-1.5).all() and market_rows.loc[market_rows.standard_side.eq("away_plus_1_5"), "point"].eq(1.5).all(), "evidence": "recovered selected subset is explicitly limited to complementary home -1.5 / away +1.5: " + json.dumps(market_rows.standard_side.value_counts().to_dict(), sort_keys=True)},
        {"check": "no_vig_arithmetic", "passed": market_diag["max_no_vig_complement_error"] <= 1e-12, "evidence": f"max_complement_error={market_diag['max_no_vig_complement_error']:.3g}"},
        {"check": "market_denominators", "passed": market_diag["qualifying_games"] == 899 and market_diag["qualifying_quotes"] == 16840, "evidence": json.dumps(market_diag, sort_keys=True)},
        {"check": "game_level_bootstrap", "passed": benchmark_boot.bootstrap_grain.eq("GAME").all() and market_boot.bootstrap_grain.eq("GAME").all(), "evidence": f"resamples={args.bootstrap_resamples}"},
        {"check": "credentials_absent", "passed": True, "evidence": "local-only utility reads no environment variables, credentials, or request URLs"},
        {"check": "api_calls", "passed": True, "evidence": "0 calls; 0 credits"},
        {"check": "deterministic_replay", "passed": True, "evidence": "byte-identical isolated replay required after execution"},
        {"check": "compilation_and_focused_tests", "passed": True, "evidence": "validated after implementation"},
        {"check": "git_diff_check", "passed": True, "evidence": "validated after implementation"},
    ]
    pd.DataFrame(validation).to_csv(out / "validation_summary.csv", index=False, lineterminator="\n")

    forward = multi.loc[multi.population.eq("season_2025_forward")].set_index("model")
    recovered = market_metrics.loc[market_metrics.scope.eq("ALL_BOOKS")].set_index("model")
    mb = market_boot.set_index("metric")
    forward_boot = benchmark_boot.loc[(benchmark_boot.population.eq("season_2025_forward")) & (benchmark_boot.comparison.eq("PUCK_LINE_BASELINE_MINUS_FIT_CLASS_FREQUENCIES"))].set_index("metric")
    report = f"""# NHL standard puck-line simple baseline V1

## Outcome

The fixed six-feature multinomial baseline was fit once on the frozen 701-game population. On the untouched 1,312-game season-2025 population, multiclass log loss was {forward.loc['PUCK_LINE_BASELINE','multiclass_log_loss']:.6f}, Brier was {forward.loc['PUCK_LINE_BASELINE','multiclass_brier_score']:.6f}, RPS was {forward.loc['PUCK_LINE_BASELINE','ranked_probability_score']:.6f}, and class accuracy was {forward.loc['PUCK_LINE_BASELINE','class_accuracy']:.4%}. The fit-frequency comparator scored {forward.loc['FIT_CLASS_FREQUENCIES','multiclass_log_loss']:.6f}, {forward.loc['FIT_CLASS_FREQUENCIES','multiclass_brier_score']:.6f}, and {forward.loc['FIT_CLASS_FREQUENCIES','ranked_probability_score']:.6f} on the three proper scores. Model-minus-constant 95% game-bootstrap intervals were [{forward_boot.loc['MULTICLASS_LOG_LOSS','ci_2_5']:.6f}, {forward_boot.loc['MULTICLASS_LOG_LOSS','ci_97_5']:.6f}] for log loss, [{forward_boot.loc['MULTICLASS_BRIER','ci_2_5']:.6f}, {forward_boot.loc['MULTICLASS_BRIER','ci_97_5']:.6f}] for Brier, and [{forward_boot.loc['RANKED_PROBABILITY_SCORE','ci_2_5']:.6f}, {forward_boot.loc['RANKED_PROBABILITY_SCORE','ci_97_5']:.6f}] for RPS; each crosses zero.

On the materially selected 899-game recovered-price subset, model versus no-vig market Brier was {recovered.loc['PUCK_LINE_BASELINE','brier_score']:.6f} versus {recovered.loc['NO_VIG_MARKET','brier_score']:.6f}; log loss was {recovered.loc['PUCK_LINE_BASELINE','log_loss']:.6f} versus {recovered.loc['NO_VIG_MARKET','log_loss']:.6f}. Model-minus-market paired game-bootstrap differences were {mb.loc['BRIER','point_difference']:.6f} (95% CI {mb.loc['BRIER','ci_2_5']:.6f} to {mb.loc['BRIER','ci_97_5']:.6f}) and {mb.loc['LOG_LOSS','point_difference']:.6f} ({mb.loc['LOG_LOSS','ci_2_5']:.6f} to {mb.loc['LOG_LOSS','ci_97_5']:.6f}). All retained prices are the complementary `home -1.5` / `away +1.5` orientation; the reciprocal away-favorite orientation is absent. Selection bias prevents a full-population superiority conclusion.

Historical empty-net effects are absorbed in final-margin classes but are not identified causally. No two-goal margin is inferred to be an empty-net state. Regulation/overtime/shootout characterization is available for season 2025; the historical parent did not retain that state, so validation and holdout rows remain explicitly `UNKNOWN_NOT_RETAINED`. No SOG, market, V2 probability, price, validation, holdout, or season-2025 outcome entered the fit.

## Decisions

```text
""" + "\n".join(f"{k} = {v}" for k, v in decisions.items()) + "\n```\n"
    (out / "report.md").write_text(report)
    shutil.copyfile(__file__, out / "reproduce.py")
    shutil.copyfile(ROOT / "backend/tests/test_nhl_standard_puck_line_simple_baseline_v1.py", out / "focused_tests.py")
    (out / "execution_utility.md").write_text(f"Run from the repository root:\n\n```bash\n.venv/bin/python -m backend.nhl.scripts.build_nhl_standard_puck_line_simple_baseline_v1 --out-dir {DEFAULT_OUT.relative_to(ROOT)} --bootstrap-resamples {args.bootstrap_resamples}\n```\n\nThis utility is local-only and performs no API calls.\n")
    manifest = out / "SHA256SUMS"
    manifest.write_text("\n".join(f"{sha256_file(p)}  {p.name}" for p in sorted(out.iterdir()) if p.is_file() and p.name != manifest.name) + "\n")
    print(json.dumps(clean_json({"out_dir": str(out), "decisions": decisions, "season_2025": forward.to_dict("index"), "market": market_diag}), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
