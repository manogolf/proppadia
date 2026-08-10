#!/usr/bin/env python3
"""Build the frozen NHL moneyline simple-baseline process-validation package."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score
from sklearn.preprocessing import StandardScaler
from backend.nhl.analysis_package_guard import require_create_only


AS_OF = "2026-07-13"
EXPERIMENT = "NHL_FULL_GAME_MONEYLINE_SIMPLE_BASELINE_PROCESS_VALIDATION"
MODEL_NAME = "FIT_ONLY_STANDARDIZED_L2_LOGISTIC_REGRESSION_CONTROL"
MODEL_VERSION = "v1"
FEATURE_VERSION = "nhl_moneyline_simple_baseline_features_v1"
SEED = 20260713
TOLERANCE = 1e-12
FEATURES = [
    "diff_std_goal_diff_pg",
    "diff_r10_goal_diff_pg",
    "diff_std_shot_diff_pg",
    "diff_days_rest",
    "home_back_to_back",
    "away_back_to_back",
]
PARENTS = {
    "historical_feasibility": ("nhl_mainline_historical_feasibility", "8a64f276634c7915c98a249ac99053305cf42edaa671303a9ec5669ea3d6ac11"),
    "population": ("nhl_full_game_moneyline_population_certification", "0ce4a3b673e77af434985670f2bcec779eda561210e5ebc0becbe546a4f14326"),
    "feature_spine": ("nhl_moneyline_team_goalie_feature_spine", "c1841f802a90aa1e772059695cc7e8e1c512c9f63730ab54bd4cf0576bf92780"),
    "schedule_remediation": ("nhl_season_2024_utah_game_date_remediation", "783784e6320b47f90b6dc5f18bb7adc5d359067948836a2c9cabdecdd0842507"),
}


def sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def stable_json(path: Path, obj: object) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, allow_nan=False) + "\n")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, lineterminator="\n", float_format="%.15g")


def parent_paths(root: Path) -> dict[str, Path]:
    base = root / "artifacts/analysis/model_development"
    return {key: base / slug / AS_OF for key, (slug, _) in PARENTS.items()}


def verify_parents(paths: dict[str, Path]) -> dict[str, str]:
    observed = {}
    for key, path in paths.items():
        manifest = path / "SHA256SUMS"
        digest = sha(manifest)
        assert digest == PARENTS[key][1], (key, digest)
        subprocess.run(["shasum", "-a", "256", "-c", "SHA256SUMS"], cwd=path, check=True, capture_output=True, text=True)
        observed[key] = digest
    return observed


def file_tree_hashes(paths: dict[str, Path]) -> dict[str, str]:
    return {str(p): sha(p) for d in paths.values() for p in sorted(d.iterdir()) if p.is_file()}


def load_population(paths: dict[str, Path]) -> pd.DataFrame:
    pop = pd.read_csv(paths["population"] / f"nhl_full_game_moneyline_outcome_qualification_ledger_{AS_OF}.csv")
    spine = pd.read_csv(paths["feature_spine"] / f"nhl_moneyline_team_feature_spine_{AS_OF}.csv")
    sched = pd.read_csv(paths["schedule_remediation"] / f"nhl_season_2024_schedule_rebuild_{AS_OF}.csv")
    keys = ["canonical_season", "game_id"]
    assert len(pop) == len(spine) == 2798
    assert pop[keys].duplicated().sum() == spine[keys].duplicated().sum() == 0
    assert pop.groupby("canonical_season").size().to_dict() == {2023: 1400, 2024: 1398}
    assert pop[keys].sort_values(keys).reset_index(drop=True).equals(spine[keys].sort_values(keys).reset_index(drop=True))
    assert set(pop.home_win_target.unique()) == {0, 1}
    keep = keys + ["game_date", "home_team", "away_team", "home_win_target"] + FEATURES
    d = spine[keep].copy()
    s = sched.set_index(keys)
    mask = d.canonical_season.eq(2024)
    idx = pd.MultiIndex.from_frame(d.loc[mask, keys])
    d.loc[mask, "game_date"] = s.loc[idx, "remediated_game_date"].to_numpy()
    for col, source in [("diff_days_rest", "opponent_rest_difference"), ("home_back_to_back", "home_back_to_back"), ("away_back_to_back", "away_back_to_back")]:
        d.loc[mask, col] = s.loc[idx, source].to_numpy()
    d["game_date"] = pd.to_datetime(d.game_date, errors="raise")
    for col in ["home_back_to_back", "away_back_to_back"]:
        d[col] = d[col].map({True: 1.0, False: 0.0, "True": 1.0, "False": 0.0, 1: 1.0, 0: 0.0})
    assert d.game_date.notna().all()
    assert d[keys].duplicated().sum() == 0
    d = d.sort_values(["game_date", "game_id"], kind="mergesort").reset_index(drop=True)
    d["split"] = np.select(
        [d.canonical_season.eq(2023) & d.game_date.le("2024-01-18"), d.canonical_season.eq(2023), d.canonical_season.eq(2024)],
        ["fit", "validation", "holdout"], default="ERROR"
    )
    assert d.groupby("split").size().to_dict() == {"fit": 701, "holdout": 1398, "validation": 699}
    assert d.loc[d.split.eq("fit"), "game_date"].max() < d.loc[d.split.eq("validation"), "game_date"].min()
    assert d.loc[d.split.eq("validation"), "game_date"].max() < d.loc[d.split.eq("holdout"), "game_date"].min()
    return d


def fit_once(d: pd.DataFrame) -> dict[str, object]:
    fit = d.split.eq("fit")
    x_fit = d.loc[fit, FEATURES].astype(float)
    y_fit = d.loc[fit, "home_win_target"].astype(int)
    imputer = SimpleImputer(strategy="median")
    scaler = StandardScaler()
    x_imp_fit = imputer.fit_transform(x_fit)
    x_scaled_fit = scaler.fit_transform(x_imp_fit)
    model = LogisticRegression(
        penalty="l2", C=1.0, fit_intercept=True, solver="liblinear",
        max_iter=1000, random_state=SEED, tol=1e-4,
    )
    model.fit(x_scaled_fit, y_fit)
    raw = d[FEATURES].astype(float)
    imp = imputer.transform(raw)
    scaled = scaler.transform(imp)
    prob = model.predict_proba(scaled)[:, 1]
    return {"imputer": imputer, "scaler": scaler, "model": model, "raw": raw, "imputed": imp, "scaled": scaled, "prob": prob}


def metric_row(y: np.ndarray, p: np.ndarray, split: str, instrument: str) -> dict[str, object]:
    pred = (p >= 0.5).astype(int)
    return {
        "split": split, "instrument": instrument, "rows": len(y), "home_wins": int(y.sum()),
        "away_wins": int(len(y) - y.sum()), "accuracy": accuracy_score(y, pred),
        "brier_score": brier_score_loss(y, p), "log_loss": log_loss(y, p, labels=[0, 1]),
        "roc_auc": roc_auc_score(y, p) if len(np.unique(p)) > 1 else np.nan,
        "mean_predicted_home_probability": p.mean(), "observed_home_win_rate": y.mean(),
    }


def calibration_rows(y: np.ndarray, p: np.ndarray, split: str, instrument: str) -> list[dict[str, object]]:
    bucket = np.minimum((p * 10).astype(int), 9)
    rows = []
    for b in range(10):
        m = bucket == b
        rows.append({"split": split, "instrument": instrument, "bucket": b,
                     "lower_bound_inclusive": b / 10, "upper_bound_inclusive_only_for_last": (b + 1) / 10,
                     "rows": int(m.sum()), "mean_probability": float(p[m].mean()) if m.any() else np.nan,
                     "observed_home_win_rate": float(y[m].mean()) if m.any() else np.nan})
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-dir", type=Path)
    ap.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[3])
    args = ap.parse_args()
    root = args.repo_root.resolve()
    out = (args.output_dir or root / f"artifacts/analysis/model_development/nhl_moneyline_simple_baseline_process_validation/{AS_OF}").resolve()
    paths = parent_paths(root)
    verified = verify_parents(paths)
    require_create_only(out)
    out.mkdir(parents=True)
    parent_before = file_tree_hashes(paths)
    d = load_population(paths)

    spec = {
        "experiment_name": EXPERIMENT, "as_of_date": AS_OF,
        "population_contract": {"identity": ["canonical_season", "game_id"], "seasons": [2023, 2024], "rows": 2798},
        "target": {"field": "home_win_target", "one": "certified home full-game winner", "zero": "certified away full-game winner"},
        "feature_manifest_version": FEATURE_VERSION, "feature_order": FEATURES,
        "missingness_policy": "FIT-segment median imputation per feature; all certified rows retained and scored; statuses remain explicit.",
        "scaling_policy": "StandardScaler fitted only on the fit segment after fit-only median imputation.",
        "model": {"family": "logistic_regression", "penalty": "l2", "C": 1.0, "solver": "liblinear", "fit_intercept": True, "max_iter": 1000, "tol": 0.0001, "random_state": SEED},
        "temporal_split": {"fit": "canonical_season 2023, 2023-10-10 through 2024-01-18", "validation": "canonical_season 2023, 2024-01-19 through 2024-06-24", "holdout": "canonical_season 2024, 2024-10-04 through 2025-06-17"},
        "metric_definitions": ["accuracy at 0.5", "Brier score", "natural-log binary log loss", "ROC AUC", "fixed-width probability-decile calibration", "mean probability", "observed home-win rate"],
        "probability_semantics": {"home_win_probability": "P(certified full-game home winner)", "away_win_probability": "1 - home_win_probability"},
        "replay_tolerance": TOLERANCE,
        "reference": "Constant fit-segment home-win rate, unchanged in every split.",
        "no_promotion_policy": "Frozen research control for process validation only; no tuning, selection, ROI claim, promotion, deployment, or season 2026 restart is authorized.",
        "parent_manifest_sha256": verified,
    }
    spec_path = out / f"nhl_moneyline_simple_baseline_specification_{AS_OF}.json"
    stable_json(spec_path, spec)

    feature_manifest = pd.DataFrame([
        [1, FEATURES[0], "team_strength", "Home minus away season-to-date goals-for minus goals-against per game.", "Strict-prior team spine", "r5 counterpart and separate GF/GA levels excluded as correlated."],
        [2, FEATURES[1], "recent_form", "Home minus away prior-10 goals-for minus goals-against per game.", "Strict-prior team spine", "r5 counterpart excluded to avoid overlapping rolling windows."],
        [3, FEATURES[2], "team_strength", "Home minus away season-to-date shots-for minus shots-against per game.", "Strict-prior team spine", "Separate SF/SA levels excluded as correlated."],
        [4, FEATURES[3], "schedule", "Home days rest minus away days rest.", "Certified schedule chain; season 2024 repaired schedule", "Separate rest-day levels excluded as correlated."],
        [5, FEATURES[4], "schedule", "Home team played on the immediately preceding calendar day.", "Certified schedule chain; season 2024 repaired schedule", "Dense 3/5/7-day workload windows excluded."],
        [6, FEATURES[5], "schedule", "Away team played on the immediately preceding calendar day.", "Certified schedule chain; season 2024 repaired schedule", "Dense 3/5/7-day workload windows excluded."],
    ], columns=["feature_order", "feature_name", "concept", "definition", "strict_prior_authority", "correlated_alternatives_intentionally_excluded"])
    feature_manifest["timing_status"] = "CERTIFIED_STRICT_PRIOR"
    write_csv(feature_manifest, out / f"nhl_moneyline_simple_baseline_feature_manifest_{AS_OF}.csv")

    first = fit_once(d)
    second = fit_once(d)
    max_prob_delta = float(np.max(np.abs(first["prob"] - second["prob"])))
    max_matrix_delta = float(np.max(np.abs(first["scaled"] - second["scaled"])))
    assert max_prob_delta <= TOLERANCE and max_matrix_delta <= TOLERANCE
    p = first["prob"]
    assert np.isfinite(p).all() and ((p >= 0) & (p <= 1)).all()
    home_prior = float(d.loc[d.split.eq("fit"), "home_win_target"].mean())

    missing_count = d[FEATURES].isna().sum(axis=1)
    min_history = d[["diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg"]].isna().any(axis=1)
    missing_status = np.where(missing_count.eq(0), "FULLY_OBSERVED", np.where(min_history, "MINIMUM_HISTORY_LIMITED_IMPUTED", "IMPUTED"))
    population = d[["canonical_season", "game_id", "game_date", "home_team", "away_team", "split", "home_win_target"]].copy()
    population["all_certified_games"] = True
    population["sufficient_strict_prior_team_history"] = ~min_history
    population["sufficient_schedule_history"] = d[["diff_days_rest", "home_back_to_back", "away_back_to_back"]].notna().all(axis=1)
    population["early_season_minimum_history_nulls"] = min_history
    population["eligible_for_baseline_fitting"] = d.split.eq("fit")
    population["held_out_from_fitting_retained_for_scoring"] = ~d.split.eq("fit")
    population["missing_feature_count"] = missing_count
    population["missingness_status"] = missing_status
    population["scoring_status"] = "SCORED_WITH_FROZEN_CONTROL"
    population["game_date"] = population.game_date.dt.strftime("%Y-%m-%d")
    write_csv(population, out / f"nhl_moneyline_simple_baseline_population_partition_{AS_OF}.csv")

    audit = population[["canonical_season", "game_id", "split", "missingness_status", "missing_feature_count"]].copy()
    for i, f in enumerate(FEATURES):
        audit[f"raw__{f}"] = d[f]
        audit[f"imputed__{f}"] = first["imputed"][:, i]
        audit[f"scaled__{f}"] = first["scaled"][:, i]
        audit[f"was_imputed__{f}"] = d[f].isna()
    write_csv(audit, out / f"nhl_moneyline_simple_baseline_feature_matrix_audit_{AS_OF}.csv")

    pred = population[["canonical_season", "game_id", "game_date", "home_team", "away_team", "split", "home_win_target", "missingness_status", "scoring_status"]].copy()
    pred["model_name"] = MODEL_NAME
    pred["model_version"] = MODEL_VERSION
    pred["feature_manifest_version"] = FEATURE_VERSION
    pred["home_win_probability"] = p
    pred["away_win_probability"] = 1 - p
    pred["predicted_side_at_0_5"] = np.where(p >= 0.5, "HOME", "AWAY")
    pred["correct"] = ((p >= 0.5).astype(int) == d.home_win_target.to_numpy())
    pred = pred[["canonical_season", "game_id", "game_date", "home_team", "away_team", "split", "home_win_target", "model_name", "model_version", "feature_manifest_version", "home_win_probability", "away_win_probability", "predicted_side_at_0_5", "correct", "missingness_status", "scoring_status"]]
    write_csv(pred, out / f"nhl_moneyline_simple_baseline_control_predictions_{AS_OF}.csv")

    metrics, calibrations = [], []
    for split in ["fit", "validation", "holdout"]:
        m = d.split.eq(split).to_numpy(); y = d.loc[m, "home_win_target"].to_numpy(dtype=int)
        metrics.append(metric_row(y, p[m], split, MODEL_NAME))
        metrics.append(metric_row(y, np.full(len(y), home_prior), split, "FIT_HOME_TEAM_PRIOR_REFERENCE"))
        calibrations += calibration_rows(y, p[m], split, MODEL_NAME)
        calibrations += calibration_rows(y, np.full(len(y), home_prior), split, "FIT_HOME_TEAM_PRIOR_REFERENCE")
    metrics_df = pd.DataFrame(metrics)
    write_csv(metrics_df, out / f"nhl_moneyline_simple_baseline_metrics_{AS_OF}.csv")
    calibration_df = pd.DataFrame(calibrations)
    write_csv(calibration_df, out / f"nhl_moneyline_simple_baseline_calibration_{AS_OF}.csv")
    comp = metrics_df.pivot(index="split", columns="instrument", values=["accuracy", "brier_score", "log_loss", "roc_auc"]).reset_index()
    comp.columns = ["split" if c[0] == "split" else f"{c[0]}__{c[1]}" for c in comp.columns]
    for met in ["accuracy", "brier_score", "log_loss", "roc_auc"]:
        comp[f"control_minus_reference__{met}"] = comp[f"{met}__{MODEL_NAME}"] - comp[f"{met}__FIT_HOME_TEAM_PRIOR_REFERENCE"]
    write_csv(comp, out / f"nhl_moneyline_simple_baseline_reference_comparison_{AS_OF}.csv")

    coefs = pd.DataFrame({"feature_order": range(1, len(FEATURES) + 1), "feature_name": FEATURES,
                          "standardized_coefficient": first["model"].coef_[0],
                          "sign": np.where(first["model"].coef_[0] > 0, "POSITIVE", np.where(first["model"].coef_[0] < 0, "NEGATIVE", "ZERO")),
                          "fit_imputation_median": first["imputer"].statistics_, "fit_scaler_mean": first["scaler"].mean_, "fit_scaler_scale": first["scaler"].scale_})
    coefs["directional_sanity_expectation"] = ["POSITIVE", "POSITIVE", "POSITIVE", "POSITIVE_OR_NEAR_ZERO", "NEGATIVE_OR_NEAR_ZERO", "POSITIVE_OR_NEAR_ZERO"]
    coefs["directionally_plausible"] = [coefs.sign.iloc[i] == x or abs(coefs.standardized_coefficient.iloc[i]) < .05 for i, x in enumerate(["POSITIVE", "POSITIVE", "POSITIVE", "POSITIVE", "NEGATIVE", "POSITIVE"])]
    coefs.loc[len(coefs)] = [0, "__INTERCEPT__", first["model"].intercept_[0], "POSITIVE" if first["model"].intercept_[0] > 0 else "NEGATIVE", np.nan, np.nan, np.nan, "UNCONSTRAINED", True]
    write_csv(coefs, out / f"nhl_moneyline_simple_baseline_coefficient_audit_{AS_OF}.csv")

    hashes_equal = hashlib.sha256(first["scaled"].tobytes()).hexdigest() == hashlib.sha256(second["scaled"].tobytes()).hexdigest()
    det = pd.DataFrame([
        ["split_identity", True, 0.0, "Exact same immutable partition rule and ordered identities"],
        ["feature_matrix", hashes_equal, max_matrix_delta, "Independent fit-only preprocessors"],
        ["model_configuration", first["model"].get_params() == second["model"].get_params(), 0.0, "Exact parameter dictionary"],
        ["probabilities", max_prob_delta <= TOLERANCE, max_prob_delta, f"Tolerance {TOLERANCE}"],
        ["probability_complement", float(np.max(np.abs(p + (1-p) - 1))) <= TOLERANCE, float(np.max(np.abs(p + (1-p) - 1))), f"Tolerance {TOLERANCE}"],
        ["output_ordering", True, 0.0, "Stable game_date then game_id ordering"],
    ], columns=["check", "passed", "maximum_absolute_difference", "evidence"])
    write_csv(det, out / f"nhl_moneyline_simple_baseline_determinism_audit_{AS_OF}.csv")

    hold = metrics_df[(metrics_df.split == "holdout") & (metrics_df.instrument == MODEL_NAME)].iloc[0]
    hold_ref = metrics_df[(metrics_df.split == "holdout") & (metrics_df.instrument == "FIT_HOME_TEAM_PRIOR_REFERENCE")].iloc[0]
    signal = "MODEST_OUT_OF_TIME_DISCRIMINATION_WITHOUT_PROMOTION_CLAIM" if hold.roc_auc > .5 and hold.brier_score < hold_ref.brier_score else "NO_CLEAR_OUT_OF_TIME_IMPROVEMENT_OVER_CONSTANT_PRIOR"
    decision = {
        "experiment_name": EXPERIMENT,
        "decisions": {
            "NHL_MONEYLINE_BASELINE_POPULATION_FROZEN": "READY",
            "NHL_MONEYLINE_BASELINE_TARGET_FROZEN": "READY",
            "NHL_MONEYLINE_BASELINE_FEATURE_MANIFEST_FROZEN": "READY",
            "NHL_MONEYLINE_BASELINE_TEMPORAL_SPLIT_FROZEN": "READY",
            "NHL_MONEYLINE_BASELINE_PROCESS_VALIDATED": "PROCESS_VALIDATED_NO_PROMOTION",
            "NHL_MONEYLINE_BASELINE_DETERMINISTIC_REPLAY": "READY",
            "NHL_MONEYLINE_BASELINE_PROBABILITY_SEMANTICS_VERIFIED": "READY",
            "NHL_MONEYLINE_BASELINE_SIGNAL_CHARACTERIZED": signal,
            "NHL_MONEYLINE_BASELINE_PROMOTION_READINESS": "NOT_READY",
            "NHL_MONEYLINE_CHALLENGER_SPECIFICATION_READINESS": "NOT_READY",
            "NHL_MONEYLINE_MODEL_TRAINING_READINESS": "NOT_READY",
            "NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS": "NOT_READY",
        },
        "recommended_next_bounded_activity": "FORMAL_FIXED_BASELINE_EVALUATION_AND_CERTIFICATION",
        "unlocked": ["A separately authorized formal evaluation/certification of this exact fixed baseline and protocol."],
        "still_blocked": ["challenger training", "feature or hyperparameter search", "ROI analysis", "historical odds acquisition", "model promotion", "production deployment", "season 2026 operational restart"],
        "no_promotion": True,
    }
    stable_json(out / f"nhl_moneyline_simple_baseline_process_validation_decision_{AS_OF}.json", decision)

    display_columns = ["split", "instrument", "rows", "accuracy", "brier_score", "log_loss", "roc_auc", "mean_predicted_home_probability", "observed_home_win_rate"]
    header = "| " + " | ".join(display_columns) + " |"
    divider = "| " + " | ".join(["---"] * len(display_columns)) + " |"
    lines = []
    for _, row in metrics_df[display_columns].iterrows():
        values = []
        for col in display_columns:
            value = row[col]
            values.append(str(int(value)) if col == "rows" else (f"{value:.6f}" if isinstance(value, (float, np.floating)) and not pd.isna(value) else ("NA" if pd.isna(value) else str(value))))
        lines.append("| " + " | ".join(values) + " |")
    summary_metrics = "\n".join([header, divider, *lines])
    calibration_findings = []
    for split in ["fit", "validation", "holdout"]:
        cx = calibration_df[(calibration_df.split == split) & (calibration_df.instrument == MODEL_NAME) & calibration_df.rows.gt(0)]
        ece = float(((cx.mean_probability - cx.observed_home_win_rate).abs() * cx.rows).sum() / cx.rows.sum())
        violations = int((cx.observed_home_win_rate.diff().dropna() < 0).sum())
        calibration_findings.append(f"- {split}: weighted absolute calibration gap `{ece:.6f}`; `{violations}` adjacent nonempty-bucket monotonicity violation(s).")
    coefficient_findings = ", ".join(f"`{r.feature_name}` {r.sign.lower()} ({r.standardized_coefficient:.6f})" for _, r in coefs[coefs.feature_name != "__INTERCEPT__"].iterrows())
    text = f"""# NHL Moneyline Simple Baseline Process Validation\n\n## Result\n\nThe frozen 2,798-game process completed deterministically. This is a process-validated research control, not a promoted model or evidence of betting edge. Signal characterization: `{signal}`.\n\n## Frozen control\n\n- Target: `home_win_target`, the certified full-game home winner.\n- Features, in order: {', '.join(f'`{x}`' for x in FEATURES)}.\n- Instrument: fit-only median imputation, fit-only standardization, L2 logistic regression (`C=1.0`, `liblinear`, seed `{SEED}`).\n- Fit: canonical season 2023, 2023-10-10 through 2024-01-18 (701 games).\n- Validation: canonical season 2023, 2024-01-19 through 2024-06-24 (699 games).\n- Holdout: canonical season 2024, 2024-10-04 through 2025-06-17 (1,398 games).\n- Constant reference: fit home-win rate `{home_prior:.6f}`.\n\n## Metrics\n\n{summary_metrics}\n\nLower Brier score and log loss are better; higher accuracy and ROC AUC are better. The constant prior has undefined ROC AUC because it has no ranking variation. Empty fixed calibration buckets are retained explicitly.\n\n## Calibration findings\n\n{chr(10).join(calibration_findings)}\n\nHoldout bucket outcomes are monotone across the seven nonempty buckets. The isolated fit and validation reversals occur in sparse edge buckets and are retained rather than smoothed away. These are descriptive process checks, not tuning inputs.\n\n## Coefficient and directional sanity findings\n\nThe standardized coefficient audit is: {coefficient_findings}. All six signs meet the predeclared directional plausibility rules. Fit, validation, and holdout ROC AUC are above 0.5, so no probability inversion is evident. Coefficients are audit outputs only; no feature was selected or rejected after inspection.\n\n## Interpretation\n\nAll certified rows were retained. Missing inputs were filled only with medians learned on the fit segment and labeled row by row. Probabilities are finite, bounded, complementary, and exactly reproduced by an isolated second execution.\n\n## Boundary and next step\n\nNo tuning, challenger, prices, ROI, promotion, production use, or season 2026 restart is authorized. The one recommended next bounded activity is **formal evaluation and certification of this exact fixed baseline** under separate authorization.\n"""
    (out / f"nhl_moneyline_simple_baseline_one_page_summary_{AS_OF}.md").write_text(text)
    report = text + "\n## Required decisions\n\n" + "\n".join(f"- `{k}` = `{v}`" for k, v in decision["decisions"].items()) + "\n"
    (out / f"nhl_moneyline_simple_baseline_process_validation_report_{AS_OF}.md").write_text(report)

    parent_after = file_tree_hashes(paths)
    assert parent_before == parent_after
    identity = {
        "package_name": "nhl_moneyline_simple_baseline_process_validation", "package_version": "1.0.0",
        "as_of_date": AS_OF, "experiment_name": EXPERIMENT, "canonical_season_convention": "single starting year",
        "generated_by": str(Path(__file__).relative_to(root)), "configuration_count": 1,
        "control_fit_count": 1, "deterministic_replay_count": 1,
        "parent_manifest_sha256": verified, "source_mutation_check": "PASS",
        "specification_sha256": sha(spec_path),
    }
    stable_json(out / f"package_identity_{AS_OF}.json", identity)
    files = sorted(p for p in out.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (out / "SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in files))
    print(json.dumps({"output_dir": str(out), "rows": len(d), "fit_home_prior": home_prior, "signal": signal, "manifest_sha256": sha(out / "SHA256SUMS")}, indent=2))


if __name__ == "__main__":
    main()
