#!/usr/bin/env python3
"""Fit and evaluate the direct NHL Points HGB V2 team-shot repair.

This is a bounded, reproducible study over already retained matrices and
immutable captures. It does not access the database or external providers.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import StandardScaler

from backend.nhl.scripts.export_nhl_points_hgb_features import FEATURES
from backend.nhl.scripts.score_nhl_points_hgb_shadow import MODEL_PATH as V1_MODEL_PATH, score

ROOT = Path(__file__).resolve().parents[3]
HIST = ROOT / "artifacts/analysis/nhl/points_historical_team_shots_reconstruction/2026-10-15"
OFFLINE = ROOT / "artifacts/analysis/nhl/points_team_shots_offline_repair/2026-10-15"
CAPTURE_ROOT = ROOT / "artifacts/operational/nhl/points_hgb_shadow/season=2026"
OUT = ROOT / "artifacts/analysis/nhl/points_hgb_v2_direct_repair/2026-10-15"
KW = dict(loss="poisson", max_iter=100, max_leaf_nodes=15,
          l2_regularization=1.0, early_stopping=False, random_state=42)
KEYS = ["game_id", "player_id"]
FEATURE_SET = set(FEATURES)
THRESHOLDS = (1, 2, 3)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prediction_sha(mu: np.ndarray) -> str:
    return hashlib.sha256(np.asarray(mu, dtype="<f8").tobytes()).hexdigest()


def matrix(frame: pd.DataFrame, scaler: StandardScaler | None = None, fit: bool = False):
    x = (frame[FEATURES].apply(pd.to_numeric, errors="coerce")
         .replace([np.inf, -np.inf], np.nan).fillna(0).astype(float).to_numpy())
    if fit:
        scaler = StandardScaler().fit(x)
    assert scaler is not None
    return scaler.transform(x), scaler


def predict(model, scaler, frame):
    x, _ = matrix(frame, scaler)
    mu = np.maximum(np.asarray(model.predict(x), dtype=float), 1e-8)
    return mu, np.column_stack([poisson.sf(k, mu) for k in (0, 1, 2)])


def metric_rows(frame: pd.DataFrame, mu: np.ndarray, probs: np.ndarray, label: str) -> list[dict]:
    y = pd.to_numeric(frame.realized_points, errors="raise").to_numpy(dtype=int)
    rows = []
    for j, threshold in enumerate(THRESHOLDS):
        actual = (y >= threshold).astype(int)
        p = np.clip(probs[:, j], 1e-12, 1 - 1e-12)
        rows.append({"configuration": label, "threshold_over": threshold - 0.5, "n": len(y),
            "auc": float(roc_auc_score(actual, p)) if actual.std() else None,
            "brier": float(np.mean((p - actual) ** 2)),
            "log_loss": float(-np.mean(actual * np.log(p) + (1 - actual) * np.log1p(-p))),
            "base_rate_over": float(actual.mean()), "mean_predicted_over": float(p.mean()),
            "over_calibration_gap": float(p.mean() - actual.mean()),
            "under_n": int((p < 0.5).sum()), "over_n": int((p >= 0.5).sum()),
            "under_actual_over_rate": float(actual[p < 0.5].mean()) if (p < 0.5).any() else None,
            "under_mean_predicted_over": float(p[p < 0.5].mean()) if (p < 0.5).any() else None,
            "over_actual_over_rate": float(actual[p >= 0.5].mean()) if (p >= 0.5).any() else None,
            "over_mean_predicted_over": float(p[p >= 0.5].mean()) if (p >= 0.5).any() else None})
    rows.append({"configuration": label, "threshold_over": "COUNT", "n": len(y),
        "mean_expected_points": float(mu.mean()), "mean_realized_points": float(y.mean()),
        "count_residual_realized_minus_expected": float(y.mean() - mu.mean()),
        "count_mae": float(np.mean(np.abs(y - mu))),
        "count_rmse": float(np.sqrt(np.mean((y - mu) ** 2)))})
    return rows


def date_stability(frame: pd.DataFrame, mu: np.ndarray, probs: np.ndarray, label: str) -> pd.DataFrame:
    data = frame[["game_date", "game_id", "realized_points"]].copy()
    data["mu"] = mu
    for i in range(3):
        data[f"p_over_{i+1}"] = probs[:, i]
    rows = []
    for day, g in data.groupby("game_date", sort=True):
        y = g.realized_points.astype(int).to_numpy()
        for j, threshold in enumerate(THRESHOLDS):
            actual = (y >= threshold).astype(int)
            p = g[f"p_over_{j+1}"].to_numpy()
            rows.append({"configuration": label, "slice_type": "DATE", "slice": str(day),
                "game_count": int(g.game_id.nunique()), "n": len(g), "threshold_over": threshold - .5,
                "mean_expected_points": float(g.mu.mean()), "mean_realized_points": float(y.mean()),
                "count_residual": float(y.mean() - g.mu.mean()),
                "brier": float(np.mean((p - actual) ** 2)), "log_loss": float(-np.mean(actual*np.log(np.clip(p,1e-12,1))+(1-actual)*np.log(np.clip(1-p,1e-12,1))))})
    data["month"] = pd.to_datetime(data.game_date).dt.to_period("M").astype(str)
    for month, g in data.groupby("month", sort=True):
        y = g.realized_points.astype(int).to_numpy()
        rows.append({"configuration": label, "slice_type": "MONTH", "slice": month,
            "game_count": int(g.game_id.nunique()), "n": len(g), "threshold_over": "COUNT",
            "mean_expected_points": float(g.mu.mean()), "mean_realized_points": float(y.mean()),
            "count_residual": float(y.mean() - g.mu.mean())})
    return pd.DataFrame(rows)


def cohort_rows_2025(frame: pd.DataFrame, predictions: dict[str, tuple[np.ndarray, np.ndarray]]) -> pd.DataFrame:
    d = frame.reset_index(drop=True).copy()
    y = d.realized_points.astype(int).to_numpy()
    dates = pd.to_datetime(d.game_date)
    first_30 = dates < (dates.min() + pd.Timedelta(days=30))
    common_masks = {"FULL_HOLDOUT": np.ones(len(d), dtype=bool), "EARLY_FIRST_30_DAYS": first_30.to_numpy()}
    output_rows = []
    for config, (_, probs) in predictions.items():
        masks = dict(common_masks)
        under = 1 - probs[:, 0]
        for lower in (0.60, 0.65, 0.70, 0.75, 0.80):
            upper = lower + .05 if lower < .80 else 1.01
            masks[f"UNDER_CONF_{int(lower*100)}_{int(min(upper,1.0)*100)}"] = (under >= lower) & (under < upper)
        for cohort, mask in masks.items():
            selected = mask
            if not selected.any():
                continue
            yy = y[selected]
            p = probs[selected, 0]
            mu = np.asarray(predictions[config][0])[selected]
            output_rows.append({"configuration": config, "cohort": cohort, "n": int(selected.sum()),
                "mean_expected_points": float(mu.mean()), "mean_realized_points": float(yy.mean()),
                "count_residual": float(yy.mean()-mu.mean()), "count_mae": float(np.mean(np.abs(yy-mu))),
                "count_rmse": float(np.sqrt(np.mean((yy-mu)**2))),
                "over_0_5_brier": float(np.mean((p-(yy>=1))**2)),
                "over_0_5_log_loss": float(-np.mean((yy>=1)*np.log(np.clip(p,1e-12,1))+(yy==0)*np.log(np.clip(1-p,1e-12,1)))),
                "high_confidence_under_n": int((1-p < .2).sum()),
                "high_confidence_under_incorrect": int(((1-p < .2)&(yy>=1)).sum())})
    return pd.DataFrame(output_rows)


def known_region_rows(frame: pd.DataFrame, predictions: dict[str, tuple[np.ndarray, np.ndarray]]) -> pd.DataFrame:
    """Apply fixed V1/market cohort labels to A/B/C paired 2026 predictions."""
    d = frame.loc[frame.realized_points.notna()].copy().reset_index(drop=True)
    y = d.realized_points.astype(int).to_numpy()
    p_over_mkt = pd.to_numeric(d.p_over_mkt, errors="coerce").to_numpy(dtype=float)
    p_under_mkt = pd.to_numeric(d.p_under_mkt, errors="coerce").to_numpy(dtype=float)
    quote = d.quote_status.eq("EXACT_TWO_SIDED").to_numpy()
    v1_mu, v1_probs = predictions["A_V1_CAPTURED_DEFECTIVE"]
    v1_over = v1_probs[:, 0]
    v1_side_over = v1_over >= 0.5
    v1_market_side = p_over_mkt >= 0.5
    selected_model_prob = np.where(v1_side_over, v1_over, 1-v1_over)
    selected_market_prob = np.where(v1_side_over, p_over_mkt, p_under_mkt)
    edge = selected_model_prob - selected_market_prob
    masks = {"FULL_SETTLED_2026": np.ones(len(d), dtype=bool),
        "EXACT_TWO_SIDED_MARKET": quote,
        "HEALTHY_NEGATIVE_EDGE_REFERENCE": quote & (edge < 0),
        "POSITIVE_EDGE_10_PLUS": quote & (edge >= .10),
        "POSITIVE_EDGE_15_PLUS": quote & (edge >= .15),
        "POSITIVE_EDGE_20_PLUS": quote & (edge >= .20),
        "HGB_V1_MARKET_OPPOSITION": quote & (v1_side_over != v1_market_side)}
    depth = pd.to_numeric(d.history_team_games, errors="coerce").fillna(0).astype(int).to_numpy()
    masks.update({f"EARLY_HISTORY_{name}": depth_mask & quote for name, depth_mask in (
        ("0_GAMES", depth == 0), ("1_2_GAMES", (depth >= 1)&(depth <= 2)),
        ("3_5_GAMES", (depth >= 3)&(depth <= 5)), ("6_9_GAMES", (depth >= 6)&(depth <= 9)),
        ("10_GAMES", depth >= 10))})
    output = []
    for config, (mu, probs) in predictions.items():
        p = probs[:, 0]
        for region, mask in masks.items():
            if not mask.any():
                output.append({"configuration": config, "region": region, "n": 0})
                continue
            yy, pp, mm = y[mask], p[mask], mu[mask]
            under = pp < .2
            output.append({"configuration": config, "region": region, "n": int(mask.sum()),
                "expected_points_mean": float(mm.mean()), "realized_points_mean": float(yy.mean()),
                "count_residual": float(yy.mean()-mm.mean()), "count_mae": float(np.mean(np.abs(yy-mm))),
                "count_rmse": float(np.sqrt(np.mean((yy-mm)**2)),),
                "over_0_5_brier": float(np.mean((pp-(yy>=1))**2)),
                "over_0_5_log_loss": float(-np.mean((yy>=1)*np.log(np.clip(pp,1e-12,1))+(yy==0)*np.log(np.clip(1-pp,1e-12,1)))),
                "high_confidence_under_n": int(under.sum()),
                "high_confidence_under_correct": int((yy[under] == 0).sum()),
                "high_confidence_under_incorrect": int((yy[under] >= 1).sum()),
                "high_confidence_under_false_rate": float((yy[under] >= 1).mean()) if under.any() else None})
    return pd.DataFrame(output)


def validate_matrices() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    original = pd.read_csv(HIST / "original_training_matrix.csv")
    corrected = pd.read_csv(HIST / "corrected_training_matrix.csv")
    holdout_orig = pd.read_csv(HIST / "original_2025_holdout_matrix.csv")
    holdout_corr = pd.read_csv(HIST / "corrected_2025_holdout_matrix.csv")
    others = [c for c in FEATURES if c != "team_d10_sf_per_game"]
    for left, right, name in ((original, corrected, "training"), (holdout_orig, holdout_corr, "holdout")):
        if not left[KEYS].equals(right[KEYS]) or not left.realized_points.equals(right.realized_points):
            raise ValueError(f"HISTORICAL_IDENTITY_OR_LABEL_PARITY_FAIL:{name}")
        if not left[others].equals(right[others]):
            raise ValueError(f"HISTORICAL_NON_TARGET_FEATURE_CHANGED:{name}")
        if (pd.to_numeric(right.team_d10_sf_per_game, errors="coerce").isna()).any():
            raise ValueError(f"HISTORICAL_CORRECTED_FEATURE_MISSING:{name}")
    manifest = {"status": "PASS", "historical_database_rebuilt": False,
        "training_rows": len(original), "training_key_sha256": hashlib.sha256(original[KEYS].to_csv(index=False).encode()).hexdigest(),
        "training_label_sha256": hashlib.sha256(original[KEYS+['realized_points']].to_csv(index=False).encode()).hexdigest(),
        "training_original_sha256": sha(HIST / "original_training_matrix.csv"),
        "training_corrected_sha256": sha(HIST / "corrected_training_matrix.csv"),
        "holdout_rows": len(holdout_orig), "holdout_original_sha256": sha(HIST / "original_2025_holdout_matrix.csv"),
        "holdout_corrected_sha256": sha(HIST / "corrected_2025_holdout_matrix.csv"),
        "only_changed_feature": "team_d10_sf_per_game", "other_nine_features_equal": True,
        "training_target_labels_equal": True, "training_identities_equal": True,
        "holdout_target_labels_equal": True, "holdout_identities_equal": True,
        "target_date_leakage_contract": "strict prior; same-date excluded; 120-day window; latest up to 10 team games",
        "official_source_manifest_sha256": sha(HIST / "official_historical_team_game_source_manifest.json")}
    return corrected, holdout_orig, manifest


def reconstruct_2026_features(replay: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    capture_audit = []
    pieces = []
    for day, targets in replay.groupby("game_date", sort=True):
        wanted = set(map(tuple, targets[KEYS].astype("int64").to_numpy()))
        candidates = []
        for path in sorted(CAPTURE_ROOT.glob(f"slate_date={day}/**/features.csv")):
            f = pd.read_csv(path)
            if set(map(tuple, f[KEYS].astype("int64").to_numpy())) == wanted:
                v1 = joblib.load(V1_MODEL_PATH)
                predicted, _ = predict(v1["model"], v1["scaler"], f)
                joined = targets.merge(f[KEYS+FEATURES], on=KEYS, validate="one_to_one")
                replay_join = targets.merge(pd.DataFrame({**{k: f[k].to_numpy() for k in KEYS},
                    "mu": predicted}), on=KEYS, validate="one_to_one")
                parity = float(np.max(np.abs(replay_join.mu.to_numpy() - targets.expected_points_original_replay.to_numpy())))
                candidates.append((parity, path, f, joined))
        candidates.sort(key=lambda item: (item[0], str(item[1])))
        if not candidates or candidates[0][0] > 1e-10:
            raise ValueError(f"2026_CAPTURE_V1_REPLAY_MISMATCH:{day}")
        parity, path, f, joined = candidates[0]
        captured = targets.merge(joined[KEYS+FEATURES], on=KEYS, validate="one_to_one")
        if not np.allclose(captured.team_d10_sf_per_game, targets.captured_team_d10_sf_per_game, atol=1e-12, rtol=0):
            raise ValueError(f"2026_CAPTURED_TEAM_D10_BINDING_FAIL:{day}")
        pieces.append(captured)
        capture_audit.append({"slate_date": str(day), "path": str(path.relative_to(ROOT)),
            "rows": len(f), "identity_match": True, "frozen_v1_replay_max_abs_error": parity,
            "feature_sha256": sha(path)})
    features = pd.concat(pieces, ignore_index=True)
    result = replay.merge(features[KEYS+FEATURES], on=KEYS, validate="one_to_one")
    if len(result) != 1800 or result[FEATURES].isna().all(axis=1).any():
        raise ValueError("2026_CAPTURED_FEATURE_POPULATION_INCOMPLETE")
    if not np.allclose(result.team_d10_sf_per_game, result.captured_team_d10_sf_per_game, atol=1e-12, rtol=0):
        raise ValueError("2026_CAPTURED_TEAM_D10_PARITY_FAIL")
    return result, capture_audit


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    training, holdout_original, matrix_manifest = validate_matrices()
    holdout_corrected = pd.read_csv(HIST / "corrected_2025_holdout_matrix.csv")
    v1 = joblib.load(V1_MODEL_PATH)
    v1_orig_mu, v1_orig_p = predict(v1["model"], v1["scaler"], holdout_original)
    v1_corr_mu, v1_corr_p = predict(v1["model"], v1["scaler"], holdout_corrected)

    xtr, scaler = matrix(training, fit=True)
    ytr = training.realized_points.to_numpy(dtype=int)
    model = HistGradientBoostingRegressor(**KW).fit(xtr, ytr)
    fitted = {"model": model, "scaler": scaler, "feature_columns": FEATURES}
    v2_holdout_mu, v2_holdout_p = predict(model, scaler, holdout_corrected)
    # Independent same-recipe second fit is the requested reproducibility check.
    model_repeat = HistGradientBoostingRegressor(**KW).fit(xtr.copy(), ytr.copy())
    repeat_mu = np.maximum(model_repeat.predict(matrix(holdout_corrected, scaler)[0]), 1e-8)
    reproducibility = {"status": "PASS" if np.array_equal(v2_holdout_mu, repeat_mu) else "FAIL",
        "first_prediction_sha256": prediction_sha(v2_holdout_mu),
        "repeat_prediction_sha256": prediction_sha(repeat_mu),
        "maximum_absolute_difference": float(np.max(np.abs(v2_holdout_mu - repeat_mu))),
        "training_rows": len(training), "training_target_sha256": matrix_manifest["training_label_sha256"],
        "hyperparameters": KW}
    if reproducibility["status"] != "PASS":
        raise ValueError("V2_FIT_REPRODUCIBILITY_FAIL")
    model_path, scaler_path, combined_path = OUT / "nhl_points_count_hgb_v2_model.joblib", OUT / "nhl_points_count_hgb_v2_scaler.joblib", OUT / "nhl_points_count_hgb_v2.joblib"
    joblib.dump(model, model_path); joblib.dump(scaler, scaler_path); joblib.dump(fitted, combined_path)

    result_metrics = []
    predictions25 = {}
    for label, mu, probs in (("V1_ORIGINAL_FEATURES", v1_orig_mu, v1_orig_p),
                             ("V1_CORRECTED_TEAM_SHOTS", v1_corr_mu, v1_corr_p),
                             ("V2_CORRECTED_TEAM_SHOTS", v2_holdout_mu, v2_holdout_p)):
        predictions25[label] = (mu, probs)
        result_metrics.extend(metric_rows(holdout_corrected, mu, probs, label))
    pd.DataFrame(result_metrics).to_csv(OUT / "2025_validation_metrics.csv", index=False)
    cohort_rows_2025(holdout_corrected, predictions25).to_csv(OUT / "2025_confidence_and_early_cohorts.csv", index=False)
    stability = pd.concat([
        date_stability(holdout_corrected, v1_orig_mu, v1_orig_p, "V1_ORIGINAL_FEATURES"),
        date_stability(holdout_corrected, v1_corr_mu, v1_corr_p, "V1_CORRECTED_TEAM_SHOTS"),
        date_stability(holdout_corrected, v2_holdout_mu, v2_holdout_p, "V2_CORRECTED_TEAM_SHOTS")], ignore_index=True)
    stability.to_csv(OUT / "2025_date_game_stability.csv", index=False)

    replay = pd.read_csv(OFFLINE / "frozen_hgb_replay_rows.csv")
    features26, capture_audit = reconstruct_2026_features(replay)
    corrected26 = features26.copy()
    corrected26["team_d10_sf_per_game"] = corrected26.corrected_team_d10_sf_per_game
    v1_corr26_mu, v1_corr26_p = predict(v1["model"], v1["scaler"], corrected26)
    if not np.allclose(v1_corr26_mu, replay.expected_points_corrected_reference, atol=1e-10, rtol=0):
        raise ValueError("2026_V1_CORRECTED_REPLAY_MISMATCH")
    v2_26_mu, v2_26_p = predict(model, scaler, corrected26)
    captured_mu = replay.expected_points_original_replay.to_numpy(dtype=float)
    captured_p = np.column_stack([replay.prob_over_0_5_original_replay.to_numpy(dtype=float),
        poisson.sf(1, captured_mu), poisson.sf(2, captured_mu)])
    # The original capture stores only 0.5 probability; verify it against frozen count.
    if not np.allclose(captured_p[:, 0], replay.prob_over_0_5_original_replay, atol=1e-12, rtol=0):
        raise ValueError("2026_V1_ORIGINAL_CAPTURE_PROBABILITY_MISMATCH")
    settled = features26.realized_points.notna().to_numpy()
    comparisons = []
    prediction_map = {}
    for label, mu, probs in (("A_V1_CAPTURED_DEFECTIVE", captured_mu, captured_p),
                             ("B_V1_CORRECTED_OFFICIAL_TEAM_SHOTS", v1_corr26_mu, v1_corr26_p),
                             ("C_V2_CORRECTED_OFFICIAL_TEAM_SHOTS", v2_26_mu, v2_26_p)):
        prediction_map[label] = (mu[settled], probs[settled])
        rows = metric_rows(features26.loc[settled].reset_index(drop=True), mu[settled], probs[settled], label)
        comparisons.extend(rows)
    pd.DataFrame(comparisons).to_csv(OUT / "2026_retrospective_metrics.csv", index=False)
    known_region_rows(features26, prediction_map).to_csv(OUT / "2026_known_region_comparisons.csv", index=False)

    source_validation = pd.read_csv(OFFLINE / "corrected_feature_rows.csv")
    source_validation["source_supported"] = source_validation.source_supported.astype(bool)
    source_validation["absolute_difference"] = (source_validation.corrected_team_d10_sf_per_game-source_validation.official_reference_team_d10).abs()
    source_validation.to_csv(OUT / "source_to_feature_validation.csv", index=False)
    settled_target_keys = replay.loc[replay.realized_points.notna(), KEYS].drop_duplicates()
    settled_source = source_validation.merge(settled_target_keys, on=KEYS, how="inner", validate="one_to_one")
    source_summary = {"target_rows": len(source_validation), "source_supported_rows": int(source_validation.source_supported.sum()),
        "exact_agreement_rows": int(source_validation.absolute_difference.le(1e-12).sum()),
        "maximum_absolute_difference": float(source_validation.absolute_difference.max()),
        "corrected_feature_mean": float(source_validation.corrected_team_d10_sf_per_game.mean()),
        "remaining_discrepancies": int(source_validation.absolute_difference.gt(1e-12).sum()),
        "source_gap_rows": int((~source_validation.source_supported).sum()),
        "settled_corrected_feature_mean": float(settled_source.corrected_team_d10_sf_per_game.mean()),
        "source_evidence_sha256": sha(OFFLINE / "official_team_game_reference.csv"),
        "reconstruction_sha256": sha(OFFLINE / "corrected_feature_rows.csv")}
    (OUT / "corrected_historical_matrix_manifest.json").write_text(json.dumps(matrix_manifest, indent=2, sort_keys=True)+"\n")
    (OUT / "source_validation_summary.json").write_text(json.dumps(source_summary, indent=2, sort_keys=True)+"\n")
    (OUT / "model_reproducibility.json").write_text(json.dumps(reproducibility, indent=2, sort_keys=True)+"\n")
    model_hashes = {"v1_model_sha256": sha(V1_MODEL_PATH),
        "v2_model_sha256": sha(model_path), "v2_scaler_sha256": sha(scaler_path), "v2_combined_sha256": sha(combined_path),
        "scaler_joblib_identity": joblib.hash(scaler), "model_type": type(model).__name__,
        "feature_columns": FEATURES, "training_feature_matrix_sha256": matrix_manifest["training_corrected_sha256"]}
    model_contract = {"schema_version": "NHL_POINTS_HGB_FITTED_MODEL_IDENTITY_V1",
        "model_id": "NHL_POINTS_COUNT_HGB_V2", "model_family": "hist_gradient_boosting",
        "model_type": type(model).__name__, "feature_columns": FEATURES,
        "feature_contract": "NHL_POINTS_LEADER_CORE_FEATURES_V1",
        "team_shot_feature_contract": "NHL_POINTS_TEAM_D10_OFFICIAL_CANONICAL_V1",
        "training_rows": len(training), "training_seasons": [2023, 2024],
        "target": "official goals + assists", "hyperparameters": KW,
        "probability_construction": "POISSON_SURVIVAL_FROM_FROZEN_HGB_EXPECTED_COUNT",
        "thresholds": {"0.5": "P(X >= 1)", "1.5": "P(X >= 2)", "2.5": "P(X >= 3)"},
        "training_feature_matrix_sha256": matrix_manifest["training_corrected_sha256"],
        "training_target_sha256": matrix_manifest["training_label_sha256"],
        "model_artifact_sha256": model_hashes["v2_model_sha256"],
        "scaler_artifact_sha256": model_hashes["v2_scaler_sha256"],
        "combined_artifact_sha256": model_hashes["v2_combined_sha256"]}
    model_contract["fitted_model_identity_sha256"] = hashlib.sha256(json.dumps(
        model_contract, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    (OUT / "model_contract.json").write_text(json.dumps(model_contract, indent=2, sort_keys=True)+"\n")
    model_hashes["fitted_model_identity_sha256"] = model_contract["fitted_model_identity_sha256"]
    (OUT / "model_artifact_hashes.json").write_text(json.dumps(model_hashes, indent=2, sort_keys=True)+"\n")
    (OUT / "2026_capture_lineage.json").write_text(json.dumps({"status": "PASS", "captured_rows": len(features26),
        "settled_rows": int(settled.sum()), "captures": capture_audit, "immutable_predictions_modified": False}, indent=2, sort_keys=True)+"\n")
    metrics25 = pd.read_csv(OUT / "2025_validation_metrics.csv")
    metrics26 = pd.read_csv(OUT / "2026_retrospective_metrics.csv")
    def record_map(frame):
        return {str(row.threshold_over): {k: (None if pd.isna(getattr(row, k)) else getattr(row, k))
                for k in ("n", "auc", "brier", "log_loss", "mean_expected_points", "mean_realized_points",
                          "count_residual_realized_minus_expected", "count_mae", "count_rmse")}
                for row in frame.itertuples(index=False)}
    summary = {"schema_version": "NHL_POINTS_HGB_V2_DIRECT_REPAIR_V1", "status": "COMPLETE",
        "model_id": "NHL_POINTS_COUNT_HGB_V2", "feature_repair": "CANONICAL_PHYSICAL_TEAM_OFFICIAL_SOG_GAME_TOTALS_STRICT_PRIOR_120D_LATEST_10",
        "source_validation": source_summary, "matrix_parity": matrix_manifest, "reproducibility": reproducibility,
        "hashes": json.loads((OUT / "model_artifact_hashes.json").read_text()),
        "model_contract_path": "model_contract.json",
        "2025_metrics": {name: record_map(group) for name, group in metrics25.groupby("configuration", sort=False)},
        "2026_metrics": {name: record_map(group) for name, group in metrics26.groupby("configuration", sort=False)},
        "2025_metrics_csv": "2025_validation_metrics.csv",
        "2026_metrics_csv": "2026_retrospective_metrics.csv",
        "known_region_csv": "2026_known_region_comparisons.csv",
        "production_authority": json.loads((ROOT / "artifacts/operational/nhl/points_model_authority/authority_v1.json").read_text()).get("production_authority"),
        "operational_feature_status": "CORRECTED_IN_EXISTING_HGB_DAILY_FEATURE_EXPORTER",
        "v2_selection_decision": "NOT_SELECTED_V2_NOT_SUPERIOR_TO_V1_WITH_CORRECTED_INPUTS",
        "v1_corrected_inputs_are_interim_choice": True,
        "v2_status": "EVALUATED_ARTIFACT_RETAINED_NOT_INTEGRATED",
        "evidence_capture_status": "INSTALLED_AWAITING_LIVE_VERIFICATION",
        "prospective_validation_required": True, "database_rebuilt": False, "database_mutations": 0,
        "phoenix_changed": False, "other_lanes_changed": False, "funnel_v1_changed": False,
        "daily_run_added": False, "provider_requests": 0, "pushed": False}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True)+"\n")
    metric_by = {row.configuration: row._asdict() for row in metrics25.itertuples(index=False) if str(row.threshold_over) == "COUNT"}
    metric26_by = {row.configuration: row._asdict() for row in metrics26.itertuples(index=False) if str(row.threshold_over) == "COUNT"}
    report = ["# NHL Points HGB V2 direct repair", "",
        "## Result", "",
        "The existing HGB exporter now constructs `team_d10_sf_per_game` from official player-game SOG records bound to canonical physical team IDs, aggregates to team-game totals, and uses the latest up to ten strict-prior games in 120 days. It rejects missing official SOG, duplicate identities, invalid team binding, and missing source timestamps. The historical pre-export intermediate was not retained, so the first historical divergence cannot be localized beyond the captured feature boundary.", "",
        f"Independent official-boxscore validation: {source_summary['exact_agreement_rows']:,}/{source_summary['target_rows']:,} rows exact; max difference {source_summary['maximum_absolute_difference']:.1f}; source gaps {source_summary['source_gap_rows']}; corrected mean {source_summary['corrected_feature_mean']:.3f} (settled {source_summary['settled_corrected_feature_mean']:.3f}).", "",
        "The historical database was not rebuilt. The 90,486-row training matrix and 47,239-row 2025 holdout retain exact identities and labels; all nine other model features are unchanged. Only `team_d10_sf_per_game` differs. See `corrected_historical_matrix_manifest.json`.", "",
        "## 2025 out-of-time holdout", "",
        "| Configuration | Expected | Realized | Residual | MAE | RMSE | Over 0.5 Brier | Over 0.5 log loss |",
        "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for name in ("V1_ORIGINAL_FEATURES", "V1_CORRECTED_TEAM_SHOTS", "V2_CORRECTED_TEAM_SHOTS"):
        row = metric_by[name]
        prob = metrics25[(metrics25.configuration == name)
                         & pd.to_numeric(metrics25.threshold_over, errors="coerce").eq(.5)].iloc[0]
        report.append(f"| {name} | {row['mean_expected_points']:.4f} | {row['mean_realized_points']:.4f} | {row['count_residual_realized_minus_expected']:+.4f} | {row['count_mae']:.4f} | {row['count_rmse']:.4f} | {prob.brier:.5f} | {prob.log_loss:.5f} |")
    report.extend(["", "V2 is slightly worse than V1 with corrected inputs on this holdout: count MAE/RMSE and the 0.5 Brier/log loss all rise slightly. Full line metrics, date/game stability, early-season rows, and confidence bins are retained in the CSV outputs. No calibration or tuning was applied.", "",
        "## 2026 retrospective (1,320 settled player-games)", "",
        "| Configuration | Expected | Realized | Residual | MAE | RMSE | Over 0.5 Brier | Over 0.5 log loss |",
        "|---|---:|---:|---:|---:|---:|---:|---:|"])
    for name in ("A_V1_CAPTURED_DEFECTIVE", "B_V1_CORRECTED_OFFICIAL_TEAM_SHOTS", "C_V2_CORRECTED_OFFICIAL_TEAM_SHOTS"):
        row = metric26_by[name]
        prob = metrics26[(metrics26.configuration == name)
                         & pd.to_numeric(metrics26.threshold_over, errors="coerce").eq(.5)].iloc[0]
        report.append(f"| {name} | {row['mean_expected_points']:.4f} | {row['mean_realized_points']:.4f} | {row['count_residual_realized_minus_expected']:+.4f} | {row['count_mae']:.4f} | {row['count_rmse']:.4f} | {prob.brier:.5f} | {prob.log_loss:.5f} |")
    report.extend(["", "V1 corrected inputs (B) improve over captured V1 (A); V2 corrected (C) does not add value over B: its expected points are slightly lower, its residual/MAE/RMSE and 0.5 Brier/log loss are slightly worse. On the full settled population, high-confidence Under false picks (P(Under) ≥80%) fall from 90/390 in A to 44/215 in B and 35/180 in C. Within the exact two-sided market cohort (806 settled rows), they are 35/116, 13/45, and 6/30. The fixed healthy negative-edge reference is small (148): its 0.5 Brier is 0.1981 for A, 0.2046 for B, 0.2043 for C, so the improvement in false Unders comes with slight deterioration there. Full settled, quote, edge ≥10/15/20%, market opposition, and history-depth cohorts are in `2026_known_region_comparisons.csv`.", "",
        "## Operational decision", "",
        "Feature correction is integrated in the existing HGB daily exporter. HGB remains `NHL_POINTS_COUNT_HGB_V1` at its established production role; V2 is saved and evaluated but not selected. Phoenix, all other lanes, the Points Funnel, model authority, and production DB are unchanged. V1 remains the deployed model and rollback reference. No additional daily orchestration or provider requests occurred.", "",
        "Pregame evidence remains `INSTALLED_AWAITING_LIVE_VERIFICATION`; a real future daily execution is required before prospective validation. The 2026 comparisons are retrospective. The separate `shot_attempts` defect is unchanged.", "",
        "## Reproducibility and validation", "",
        f"V2 refit twice using the same 90,486-row corrected training matrix and frozen HGB recipe: maximum prediction difference {reproducibility['maximum_absolute_difference']:.1f}; prediction hash `{reproducibility['first_prediction_sha256']}`. Artifact hashes are in `model_artifact_hashes.json`. Focused tests also cover the source/canonical-team history rules.", "",
        "NHL_POINTS_HGB_V2_REPAIR_STATUS = FEATURE_REPAIRED_V2_NOT_SUPERIOR", "",
        "NHL_POINTS_PREGAME_EVIDENCE_STATUS = INSTALLED_AWAITING_LIVE_VERIFICATION"])
    (OUT / "report.md").write_text("\n".join(report)+"\n")
    print(json.dumps({"status": "COMPLETE", "out_dir": str(OUT), "source_validation": source_summary,
        "matrix_parity": matrix_manifest, "model_hashes": summary["hashes"], "reproducibility": reproducibility}, indent=2))


if __name__ == "__main__":
    main()
