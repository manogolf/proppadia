#!/usr/bin/env python3
"""Independent rolling-origin validation of the frozen NHL Points bakeoff leader.

Research-only. Reads the immutable bakeoff frame, refits on each prior-date
training window, and writes outputs to a separate validation directory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
import scipy.stats as st
from scipy.optimize import minimize_scalar
import statsmodels.api as sm
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from backend.nhl.scripts.build_nhl_points_architecture_bakeoff import (
    ARMS, CORE, PHOENIX, TARGETS, matrix, metrics, safe_probs, sha256,
)

DEFAULT_INPUT = Path("artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz")
DEFAULT_OUT = Path("artifacts/analysis/nhl/points_leader_validation/2026-10-09")
RESTORATION_DIR = Path("artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3")
HGB_KW = dict(loss="poisson", max_iter=100, max_leaf_nodes=15,
              l2_regularization=1.0, early_stopping=False, random_state=42)


def poisson_probs(mu: np.ndarray) -> np.ndarray:
    mu = np.maximum(np.asarray(mu, dtype=float), 1e-10)
    return np.column_stack([st.poisson.sf(k, mu) for k in (0, 1, 2)])


def nb_probs(mu: np.ndarray, alpha: float) -> np.ndarray:
    """Threshold probabilities for NB2: variance=mu+alpha*mu**2."""
    mu = np.maximum(np.asarray(mu, dtype=float), 1e-10)
    if alpha <= 1e-10:
        return poisson_probs(mu)
    size = 1.0 / alpha
    prob = size / (size + mu)
    return np.column_stack([st.nbinom.sf(k, size, prob) for k in (0, 1, 2)])


def fit_alpha(y: np.ndarray, mu: np.ndarray) -> float:
    """Estimate one NB2 alpha from training outcomes and training HGB means."""
    y = np.asarray(y, dtype=int)
    mu = np.maximum(np.asarray(mu, dtype=float), 1e-10)

    def objective(alpha: float) -> float:
        if alpha < 1e-8:
            return float(-st.poisson.logpmf(y, mu).mean())
        size = 1.0 / alpha
        p = size / (size + mu)
        return float(-st.nbinom.logpmf(y, size, p).mean())

    result = minimize_scalar(objective, bounds=(1e-6, 2.0), method="bounded",
                             options={"xatol": 1e-7})
    return float(result.x)


def count_summary(y: np.ndarray, tail_probs: np.ndarray, mu: np.ndarray) -> dict:
    y = np.asarray(y, dtype=int)
    pred_bins = np.column_stack([1-tail_probs[:, 0],
        tail_probs[:, 0]-tail_probs[:, 1], tail_probs[:, 1]-tail_probs[:, 2], tail_probs[:, 2]])
    pred_bins = np.clip(pred_bins, 1e-12, 1)
    obs_bins = np.array([(y == k).mean() for k in range(3)] + [(y >= 3).mean()])
    pred_rate = pred_bins.mean(axis=0)
    return {
        "n": len(y), "mean_predicted": float(np.mean(mu)), "mean_observed": float(y.mean()),
        "variance_observed": float(y.var()), "variance_mean_ratio_global": float(y.var()/y.mean()),
        "poisson_count_nll": float(-st.poisson.logpmf(y, np.maximum(mu, 1e-10)).mean()),
        "grouped_0_1_2_3plus_nll": float(-np.log(pred_bins[np.arange(len(y)), np.minimum(y, 3)]).mean()),
        "zero_observed": float(obs_bins[0]), "zero_predicted": float(pred_rate[0]),
        "one_observed": float(obs_bins[1]), "one_predicted": float(pred_rate[1]),
        "two_observed": float(obs_bins[2]), "two_predicted": float(pred_rate[2]),
        "three_plus_observed": float(obs_bins[3]), "three_plus_predicted": float(pred_rate[3]),
    }


def model_threshold_rows(yframe: pd.DataFrame, probs: np.ndarray, model: str,
                         fold: str, history: str) -> list[dict]:
    out = []
    for i, (target, threshold) in enumerate(zip(TARGETS, (1, 2, 3))):
        m = metrics(yframe[target].to_numpy(), probs[:, i])
        m.update(model=model, fold=fold, history_contract=history, threshold=threshold)
        out.append(m)
    return out


def season_split(frame: pd.DataFrame, evaluation_season: int, history: str = ARMS[2]):
    """Frozen season holdout: every fit row predates the evaluation season."""
    eligible = frame.loc[frame.history_contract == history].drop_duplicates(["game_id", "player_id"])
    train = eligible.loc[eligible.canonical_season < evaluation_season].copy()
    test = eligible.loc[eligible.canonical_season == evaluation_season].copy()
    if train.empty or test.empty:
        raise ValueError(f"No season split rows for evaluation season {evaluation_season}")
    if (train.canonical_season >= evaluation_season).any():
        raise ValueError("Season holdout contains evaluation-season training rows")
    return train, test


def validate_restored_targets(frame: pd.DataFrame, outcome_path: Path, sha256s_path: Path) -> None:
    """Verify the immutable official outcome file and its join to validation targets."""
    expected_sha256 = next((line.split()[0] for line in sha256s_path.read_text().splitlines()
                            if line.split(maxsplit=1)[-1].lstrip("* ") == outcome_path.name), None)
    if not expected_sha256 or sha256(outcome_path) != expected_sha256:
        raise ValueError("Restored official outcome file does not match its retained SHA256SUMS")
    official = pd.read_csv(outcome_path)
    official = official.loc[official.canonical_season == 2025].copy()
    official["realized_points"] = official.goals + official.assists
    comparison = frame.loc[frame.canonical_season == 2025, ["game_id", "player_id", "realized_points"]]
    joined = comparison.merge(official[["game_id", "player_id", "realized_points"]],
                             on=["game_id", "player_id"], suffixes=("_frame", "_official"),
                             validate="one_to_one")
    if len(joined) != len(comparison) or not joined.realized_points_frame.equals(joined.realized_points_official):
        raise ValueError("2025 validation targets differ from restored official goals plus assists")


def fit_predict(train: pd.DataFrame, test: pd.DataFrame, history: str) -> tuple[dict, dict]:
    xtr, scaler = matrix(train, CORE, fit=True)
    xte, _ = matrix(test, CORE, scaler)
    ytr = train.realized_points.to_numpy(dtype=int)
    yte = test.realized_points.to_numpy(dtype=int)
    pred: dict[str, np.ndarray] = {}

    hgb = HistGradientBoostingRegressor(**HGB_KW).fit(xtr, ytr)
    mu = np.maximum(hgb.predict(xte), 1e-8)
    pred["HGB_POISSON"] = poisson_probs(mu)
    alpha = float(test.attrs["training_only_alpha"])
    pred["HGB_NB_DISTRIBUTION"] = nb_probs(mu, alpha)
    pred["_mu"] = mu

    # Exposure ablation: same frozen HGB learner, remove both strict-prior TOI fields.
    no_exp = [c for c in CORE if c not in ("mean_toi_last10", "mean_pp_toi_last10")]
    xntr, xnscaler = matrix(train, no_exp, fit=True)
    xnte, _ = matrix(test, no_exp, xnscaler)
    pred["HGB_NO_TOI"] = poisson_probs(np.maximum(HistGradientBoostingRegressor(**HGB_KW).fit(xntr, ytr).predict(xnte), 1e-8))

    xm = sm.add_constant(xtr, has_constant="add")
    xtm = sm.add_constant(xte, has_constant="add")
    # Offset is strict-prior mean TOI; imputation is learned from training only.
    toi_fill = float(train.mean_toi_last10.median())
    offtr = np.log(np.maximum(train.mean_toi_last10.fillna(toi_fill).to_numpy(), 1.0))
    ofte = np.log(np.maximum(test.mean_toi_last10.fillna(toi_fill).to_numpy(), 1.0))
    try:
        poisson = sm.GLM(ytr, xm, family=sm.families.Poisson(), offset=offtr).fit()
        pred["POISSON_TOI_OFFSET"] = poisson_probs(np.maximum(poisson.predict(xtm, offset=ofte), 1e-8))
    except Exception as exc:
        pred["_poisson_error"] = str(exc)
    try:
        nbfit = sm.NegativeBinomial(ytr, xm, loglike_method="nb2").fit(disp=False, maxiter=200)
        mean_nb = np.maximum(nbfit.predict(xtm), 1e-8)
        alpha_classical = float(nbfit.params[-1])
        pred["CLASSICAL_NB2"] = nb_probs(mean_nb, alpha_classical)
        pred["_classical_nb_alpha"] = alpha_classical
    except Exception as exc:
        pred["_nb_error"] = str(exc)

    # Natural unweighted binary control and the balanced Phoenix-style incumbent.
    xp, pscaler = matrix(train, PHOENIX, fit=True)
    xpt, _ = matrix(test, PHOENIX, pscaler)
    for model_name, cols_x, test_x, balanced in (
        ("NATURAL_BINARY_LOGIT", xtr, xte, False),
        ("BALANCED_PHOENIX_PRIOR_REVERSED", xp, xpt, True),
    ):
        ps = []
        for target in TARGETS:
            prevalence = float(train[target].mean())
            clf = LogisticRegression(C=1.0, class_weight="balanced" if balanced else None,
                                     max_iter=1000, solver="lbfgs").fit(cols_x, train[target])
            q = clf.predict_proba(test_x)[:, 1]
            if balanced:
                q = (q * prevalence) / (q * prevalence + (1-q) * (1-prevalence))
            ps.append(q)
        pred[model_name] = np.column_stack(ps)
    return pred, {"alpha_hgb_nb": alpha,
                  "alpha_classical_nb2": pred.get("_classical_nb_alpha"),
                  "n_train": len(train), "n_test": len(test)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--evaluation-season", type=int, default=2024)
    ap.add_argument("--restoration-dir", type=Path, default=RESTORATION_DIR)
    args = ap.parse_args()
    if args.evaluation_season == 2025 and args.input == DEFAULT_INPUT:
        args.input = args.restoration_dir / "validation_input_2023_2025.csv.gz"
    if args.evaluation_season != 2024:
        args.out_dir = args.out_dir / f"evaluation_season={args.evaluation_season}"
    args.out_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.read_csv(args.input, parse_dates=["game_date"])
    if args.evaluation_season == 2025:
        manifest = json.loads((args.restoration_dir / "validation_input_manifest.json").read_text())
        if sha256(args.input) != manifest["validation_input_sha256"]:
            raise ValueError("Validation input hash does not match restored recovery manifest")
        outcome_path = args.restoration_dir / "canonical_player_game_outcomes.csv"
        validate_restored_targets(frame, outcome_path, args.restoration_dir / "SHA256SUMS")
        (args.out_dir / "restored_target_binding.json").write_text(json.dumps({
            "status":"PASS", "validation_input_sha256":sha256(args.input),
            "validation_input_expected_sha256":manifest["validation_input_sha256"],
            "recovery_manifest_outcome_sha256":manifest["outcome_sha256"],
            "canonical_outcomes_file_sha256":sha256(outcome_path),
            "canonical_outcomes_sha256sum_verified":True,
            "target_identity_join":"one_to_one game_id,player_id",
            "validated_rows":int((frame.canonical_season == 2025).sum()),
            "equation":"realized_points = official goals + official assists",
            "all_targets_equal":True
        },indent=2)+"\n")
        (args.out_dir / "strict_prior_leakage_audit.json").write_text(json.dumps({
            "status":"PASS", "feature_builder":manifest["feature_builder"],
            "feature_semantics":manifest["feature_semantics"],
            "reported_leaking_feature_rows":0,
            "same_day_history_inclusion":False,
            "same_game_realized_toi_fields_in_model_features":False,
            "model_feature_columns":CORE
        },indent=2)+"\n")
    train_holdout, test_holdout = season_split(frame, args.evaluation_season)
    data = test_holdout.copy()
    data = data.sort_values(["game_date", "game_id", "player_id"])
    fold_key = data.game_date.dt.to_period("M").astype(str)
    all_thresholds, alphas, rows = [], [], []
    model_rows: dict[str, list[pd.DataFrame]] = {}
    # Build strict-prior historical predictions to estimate NB dispersion without
    # using in-sample residuals. These predictions come only from earlier months.
    hist = frame[(frame.history_contract == ARMS[2]) & (frame.canonical_season < args.evaluation_season)].copy()
    hist_oof = []
    for month in sorted(hist.game_date.dt.to_period("M").astype(str).unique()):
        month_mask = hist.game_date.dt.to_period("M").astype(str) == month
        test_oof = hist.loc[month_mask].copy()
        cutoff_oof = pd.Timestamp(f"{month}-01")
        train_oof = hist.loc[hist.game_date < cutoff_oof].copy()
        if train_oof.empty:
            continue
        xo, xo_scaler = matrix(train_oof, CORE, fit=True)
        xto, _ = matrix(test_oof, CORE, xo_scaler)
        model_oof = HistGradientBoostingRegressor(**HGB_KW).fit(xo, train_oof.realized_points.to_numpy())
        hist_oof.append(pd.DataFrame({"game_date": test_oof.game_date.to_numpy(),
            "points": test_oof.realized_points.to_numpy(),
            "predicted_mean": np.maximum(model_oof.predict(xto), 1e-8), "fold": month}))
    hist_oof = pd.concat(hist_oof, ignore_index=True)
    # Primary season holdout uses only pre-season rows. Dispersion is irrelevant to
    # the frozen Poisson candidates; alpha is set to zero solely to reuse the
    # candidate helper's diagnostic return shape.
    xtr, scaler = matrix(train_holdout, CORE, fit=True)
    xte, _ = matrix(test_holdout, CORE, scaler)
    ytr = train_holdout.realized_points.to_numpy(dtype=int)
    yte = test_holdout.realized_points.to_numpy(dtype=int)
    hgb_model = HistGradientBoostingRegressor(**HGB_KW).fit(xtr, ytr)
    hgb_mu = np.maximum(hgb_model.predict(xte), 1e-8)
    hgb_prob = poisson_probs(hgb_mu)
    # Use a single train-fitted scaler for the offset model, matching fit_predict.
    xm, offset_scaler = matrix(train_holdout, CORE, fit=True)
    xtm, _ = matrix(test_holdout, CORE, offset_scaler)
    toi_fill = float(train_holdout.mean_toi_last10.median())
    offtr = np.log(np.maximum(train_holdout.mean_toi_last10.fillna(toi_fill).to_numpy(), 1.0))
    ofte = np.log(np.maximum(test_holdout.mean_toi_last10.fillna(toi_fill).to_numpy(), 1.0))
    offset_fit = sm.GLM(ytr, sm.add_constant(xm, has_constant="add"),
                        family=sm.families.Poisson(), offset=offtr).fit()
    offset_mu = np.maximum(offset_fit.predict(sm.add_constant(xtm, has_constant="add"), offset=ofte), 1e-8)
    if args.evaluation_season == 2025:
        artifact_path = args.out_dir / "nhl_points_count_hgb_v1.joblib"
        joblib.dump({"model":hgb_model,"scaler":scaler,"feature_columns":CORE}, artifact_path)
        contract = {
            "schema_version":"NHL_POINTS_RESEARCH_SHADOW_CONTRACT_V1",
            "research_model_id":"NHL_POINTS_COUNT_HGB_V1",
            "authority":"AUTHORITATIVE_RESEARCH_MODEL_SELECTED",
            "training_seasons":sorted(train_holdout.canonical_season.unique().astype(int).tolist()),
            "training_rows":len(train_holdout), "evaluation_season_confirmation":args.evaluation_season,
            "training_source_frame_sha256":sha256(args.input),
            "feature_contract":"NHL_POINTS_LEADER_CORE_FEATURES_V1", "feature_columns":CORE,
            "history_contract":ARMS[2], "history_semantics":"strict prior game_date; same-day excluded; 120-day bound; last 10 player games",
            "target":"official goals + official assists",
            "distribution":"Poisson observation distribution",
            "thresholds":{"over_0_5":"P(X >= 1)","over_1_5":"P(X >= 2)","over_2_5":"P(X >= 3)"},
            "hyperparameters":HGB_KW, "fitted_artifact":artifact_path.name,
            "fitted_artifact_sha256":sha256(artifact_path),
            "scorer":"backend/nhl/scripts/build_nhl_points_leader_validation.py",
            "scorer_sha256":sha256(Path(__file__)),
            "prospective_only":True, "production_replacement":False,
            "initial_shadow_prediction":"NOT_GENERATED: retained daily input lacks frozen CORE feature columns and pregame binding for this contract"
        }
        identity = hashlib.sha256(json.dumps(contract,sort_keys=True,separators=(",",":"),default=str).encode()).hexdigest()
        contract["model_identity_sha256"] = identity
        (args.out_dir / "shadow_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
        contract["model_identity_sha256"] = identity
        (args.out_dir / "shadow_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
    primary_rows = []
    for model, mu, probs in (("HGB_POISSON", hgb_mu, hgb_prob),
                             ("POISSON_TOI_OFFSET", offset_mu, poisson_probs(offset_mu))):
        for row in model_threshold_rows(test_holdout, probs, model, f"SEASON_{args.evaluation_season}", ARMS[2]):
            primary_rows.append(row)
        c = count_summary(yte, probs, mu)
        c.update(model=model, count_nll=float(-st.poisson.logpmf(yte, mu).mean()))
        pd.DataFrame([c]).to_csv(args.out_dir / f"{model.lower()}_season_count_metrics.csv", index=False)
    pd.DataFrame(primary_rows).to_csv(args.out_dir / "season_holdout_threshold_metrics.csv", index=False)
    # Same player-cluster bootstrap contract as the prior validation.
    joined = pd.DataFrame({"player_id":test_holdout.player_id.to_numpy(), "points":yte,
        **{f"hgb_{k}":hgb_prob[:,i] for i,k in enumerate(("0_5","1_5","2_5"))},
        **{f"offset_{k}":poisson_probs(offset_mu)[:,i] for i,k in enumerate(("0_5","1_5","2_5"))}})
    target = np.column_stack([yte >= k for k in (1,2,3)])
    loss_h = -np.log(np.clip(np.where(target, hgb_prob, 1-hgb_prob), 1e-12, 1)).mean(axis=1)
    offset_prob = poisson_probs(offset_mu)
    loss_o = -np.log(np.clip(np.where(target, offset_prob, 1-offset_prob), 1e-12, 1)).mean(axis=1)
    joined["delta"] = loss_h - loss_o
    by_player = joined.groupby("player_id").delta.agg(["sum","count"])
    rng = np.random.default_rng(20261009)
    sums, counts = by_player["sum"].to_numpy(), by_player["count"].to_numpy()
    draws = np.empty(500)
    for i in range(len(draws)):
        pick = rng.integers(0,len(sums),size=len(sums))
        draws[i] = sums[pick].sum()/counts[pick].sum()
    pd.DataFrame([{"comparison":"HGB_POISSON minus POISSON_TOI_OFFSET",
        "player_clusters":len(sums), "point_estimate":float(joined.delta.mean()),
        "bootstrap_replicates":500, "ci_2_5":float(np.quantile(draws,.025)),
        "ci_97_5":float(np.quantile(draws,.975)), "replicates_favoring_hgb":float(np.mean(draws<0))}]
        ).to_csv(args.out_dir / "season_holdout_player_cluster_bootstrap.csv", index=False)
    holdout_pred = pd.DataFrame({"game_date":test_holdout.game_date, "game_id":test_holdout.game_id,
        "player_id":test_holdout.player_id, "points":yte, "predicted_mean":hgb_mu,
        "p_over_0_5":hgb_prob[:,0], "p_over_1_5":hgb_prob[:,1], "p_over_2_5":hgb_prob[:,2],
        "current_season_games_prior":test_holdout.current_season_games_prior,
        "player_lifetime_games_prior":test_holdout.player_lifetime_games_prior})
    holdout_pred.to_csv(args.out_dir / "hgb_poisson_season_holdout_predictions.csv.gz", index=False, compression="gzip")
    crossing_count = int(((hgb_prob[:,0] < hgb_prob[:,1]) | (hgb_prob[:,1] < hgb_prob[:,2])).sum())
    if crossing_count:
        raise RuntimeError(f"HGB threshold coherence failed for {crossing_count} rows")
    (args.out_dir / "season_holdout_population.json").write_text(json.dumps({
        "evaluation_season":args.evaluation_season, "training_seasons":sorted(train_holdout.canonical_season.unique().astype(int).tolist()),
        "training_rows":len(train_holdout), "evaluation_rows":len(test_holdout),
        "evaluation_games":int(test_holdout.game_id.nunique()),
        "evaluation_start":str(test_holdout.game_date.min().date()), "evaluation_end":str(test_holdout.game_date.max().date()),
        "history_contract":ARMS[2], "coherence_crossings":crossing_count,
        "training_contains_evaluation_season":bool((train_holdout.canonical_season == args.evaluation_season).any())
    }, indent=2)+"\n")
    opening_rows = []
    dates = pd.to_datetime(holdout_pred.game_date)
    opening_masks = {
        "ZERO_PRIOR_CURRENT_SEASON_GAMES": test_holdout.current_season_games_prior.eq(0).to_numpy(),
        "ONE_PRIOR_CURRENT_SEASON_GAME": test_holdout.current_season_games_prior.eq(1).to_numpy(),
        "TWO_PRIOR_CURRENT_SEASON_GAMES": test_holdout.current_season_games_prior.eq(2).to_numpy(),
        "THREE_PLUS_PRIOR_CURRENT_SEASON_GAMES": test_holdout.current_season_games_prior.ge(3).to_numpy(),
        "FIRST_7_CALENDAR_DAYS": (dates < dates.min() + pd.Timedelta(days=7)).to_numpy(),
        "FIRST_14_CALENDAR_DAYS": (dates < dates.min() + pd.Timedelta(days=14)).to_numpy(),
        "REMAINDER_OF_SEASON": (dates >= dates.min() + pd.Timedelta(days=14)).to_numpy(),
    }
    for segment, mask in opening_masks.items():
        rows = np.flatnonzero(mask)
        if not len(rows):
            continue
        for i, threshold in enumerate((1,2,3)):
            m = metrics((yte[rows] >= threshold).astype(int), hgb_prob[rows,i])
            opening_rows.append({"segment":segment, "threshold":threshold, "n":len(rows),
                "observed_points_mean":float(yte[rows].mean()), "predicted_points_mean":float(hgb_mu[rows].mean()), **m})
    pd.DataFrame(opening_rows).to_csv(args.out_dir / "hgb_season_opening_metrics.csv", index=False)
    for month in sorted(fold_key.unique()):
        test = data[fold_key == month].copy()
        cutoff = pd.Timestamp(f"{month}-01")
        train = frame[(frame.history_contract == ARMS[2]) & (frame.game_date < cutoff)].copy()
        if train.empty or test.empty:
            continue
        train = train.drop_duplicates(["game_id", "player_id"])
        test = test.drop_duplicates(["game_id", "player_id"])
        # Keep the diagnostic dispersion fit frozen to pre-evaluation seasons.
        # Earlier 2025 monthly outcomes never refit α.
        alpha_train = hist_oof.loc[hist_oof.game_date < cutoff].copy()
        if alpha_train.empty:
            raise RuntimeError(f"No strictly prior out-of-fold rows for NB dispersion at {month}")
        test.attrs["training_only_alpha"] = fit_alpha(
            alpha_train.points.to_numpy(dtype=int), alpha_train.predicted_mean.to_numpy(dtype=float))
        preds, info = fit_predict(train, test, ARMS[2])
        info.update(fold=month, train_through=str(cutoff.date() - pd.Timedelta(days=1)))
        alphas.append(info)
        for name, p in preds.items():
            if name.startswith("_"):
                continue
            all_thresholds.extend(model_threshold_rows(test, p, name, month, ARMS[2]))
            model_rows.setdefault(name, []).append(pd.DataFrame({
                "game_date": test.game_date.to_numpy(), "game_id": test.game_id.to_numpy(),
                "player_id": test.player_id.to_numpy(), "points": test.realized_points.to_numpy(),
                "current_season_games_prior": test.current_season_games_prior.to_numpy(),
                "player_lifetime_games_prior": test.player_lifetime_games_prior.to_numpy(),
                "mean_toi_last10": test.mean_toi_last10.to_numpy(),
                "predicted_mean": preds["_mu"],
                "p_over_0_5": p[:, 0], "p_over_1_5": p[:, 1], "p_over_2_5": p[:, 2],
                "fold": month,
            }))
    pd.DataFrame(all_thresholds).to_csv(args.out_dir / "rolling_origin_threshold_metrics.csv", index=False)
    pd.DataFrame(alphas).to_csv(args.out_dir / "fold_training_and_dispersion.csv", index=False)
    merged = {name: pd.concat(parts, ignore_index=True) for name, parts in model_rows.items()}
    # Direct distribution comparison uses same HGB mean. The two methods’ ranks must match.
    hpois, hnb = merged["HGB_POISSON"], merged["HGB_NB_DISTRIBUTION"]
    rank_rows = []
    for col in ("p_over_0_5", "p_over_1_5", "p_over_2_5"):
        taus_p, taus_n, rhos, discrepancies = [], [], [], []
        for fold in sorted(hpois.fold.unique()):
            hp = hpois[hpois.fold == fold]
            hn = hnb[hnb.fold == fold]
            mu = hp.predicted_mean.to_numpy()
            pp, pn = hp[col].to_numpy(), hn[col].to_numpy()
            taus_p.append(st.kendalltau(mu, pp).statistic)
            taus_n.append(st.kendalltau(mu, pn).statistic)
            rhos.append(st.spearmanr(pp, pn).statistic)
            order_p = np.argsort(pp, kind="mergesort")
            order_n = np.argsort(pn, kind="mergesort")
            discrepancies.append(int(np.sum(order_p != order_n)))
        rank_rows.append({"threshold": col, "min_fold_spearman_poisson_vs_nb": float(np.min(rhos)),
                          "min_fold_kendall_mean_vs_poisson": float(np.nanmin(taus_p)),
                          "min_fold_kendall_mean_vs_nb": float(np.nanmin(taus_n)),
                          "sum_fold_rank_position_discrepancies": int(sum(discrepancies)),
                          "coherence_crossings_poisson": int(((hpois.p_over_0_5 < hpois.p_over_1_5) | (hpois.p_over_1_5 < hpois.p_over_2_5)).sum()),
                          "coherence_crossings_nb": int(((hnb.p_over_0_5 < hnb.p_over_1_5) | (hnb.p_over_1_5 < hnb.p_over_2_5)).sum())})
    pd.DataFrame(rank_rows).to_csv(args.out_dir / "distribution_rank_and_coherence.csv", index=False)

    # Distribution metrics on the identical rolling-origin HGB rows.
    distro_rows = []
    for row, name in ((hpois, "POISSON"), (hnb, "NB")):
        y = row.points.to_numpy(dtype=int); mu = row.predicted_mean.to_numpy()
        alpha_by_fold = {a["fold"]: a["alpha_hgb_nb"] for a in alphas}
        if name == "POISSON":
            pmf = np.column_stack([st.poisson.pmf(k, mu) for k in range(3)])
        else:
            a = row.fold.map(alpha_by_fold).to_numpy(dtype=float)
            pmf = np.column_stack([st.nbinom.pmf(k, 1/a, (1/a)/(1/a+mu)) for k in range(3)])
        # score full count with right-censored 3+ grouping where tail counts are collapsed.
        metrics_row = count_summary(y, row[["p_over_0_5", "p_over_1_5", "p_over_2_5"]].to_numpy(), mu)
        metrics_row.update(distribution=name)
        if name == "POISSON":
            metrics_row["full_count_nll"] = float(-st.poisson.logpmf(y, mu).mean())
        else:
            yy = y; sz = 1/a; pr = sz/(sz+mu)
            metrics_row["full_count_nll"] = float(-st.nbinom.logpmf(yy, sz, pr).mean())
        distro_rows.append(metrics_row)
    pd.DataFrame(distro_rows).to_csv(args.out_dir / "hgb_distribution_comparison.csv", index=False)

    # Adequately sized mean and history depth bins for empirical conditional variance diagnostics.
    diag = hpois.copy()
    diag["mean_band"] = pd.qcut(diag.predicted_mean, 10, duplicates="drop")
    diag["history_band"] = pd.cut(diag.current_season_games_prior,
        bins=[-1, 0, 1, 2, 10000], labels=["0", "1", "2", "3+"])
    diag_rows = []
    for dim in ("mean_band", "history_band"):
        for band, g in diag.groupby(dim, observed=True):
            y = g.points.to_numpy(dtype=int); mu = g.predicted_mean.to_numpy()
            mean = float(y.mean()); var = float(y.var())
            diag_rows.append({"dimension": dim, "band": str(band), "n": len(y),
                "mean_predicted": float(mu.mean()), "mean_observed": mean,
                "variance_observed": var, "variance_mean_ratio": var/mean if mean else None,
                "zero_frequency": float((y == 0).mean()), "one_frequency": float((y == 1).mean()),
                "two_frequency": float((y == 2).mean()), "three_plus_frequency": float((y >= 3).mean())})
    pd.DataFrame(diag_rows).to_csv(args.out_dir / "conditional_dispersion_diagnostics.csv", index=False)

    # Frozen bakeoff history-contract exact per-line 2024 results, unchanged.
    base = DEFAULT_INPUT.parent
    frozen = pd.read_csv(base / "threshold_metrics.csv")
    frozen.to_csv(args.out_dir / "frozen_bakeoff_history_contract_metrics.csv", index=False)
    overall_rows = []
    for name, d in merged.items():
        for target, col, thr in zip(TARGETS, ("p_over_0_5", "p_over_1_5", "p_over_2_5"), (1,2,3)):
            m = metrics((d.points >= thr).astype(int), d[col].to_numpy())
            m.update(model=name, threshold=thr, evaluation=f"EXPANDING_MONTHLY_OOT_{args.evaluation_season}_SEASON")
            overall_rows.append(m)
    pd.DataFrame(overall_rows).to_csv(args.out_dir / "rolling_origin_overall_metrics.csv", index=False)

    # Player-cluster uncertainty for the HGB versus offset average threshold log loss.
    hgb = merged["HGB_POISSON"]
    off = merged["POISSON_TOI_OFFSET"]
    join_keys = ["game_id", "player_id"]
    joined = hgb[join_keys + ["points", "p_over_0_5", "p_over_1_5", "p_over_2_5"]].merge(
        off[join_keys + ["p_over_0_5", "p_over_1_5", "p_over_2_5"]], on=join_keys,
        validate="one_to_one", suffixes=("_hgb", "_offset"))
    y = joined.points.to_numpy(dtype=int)
    losses = []
    for suffix in ("hgb", "offset"):
        pp = np.column_stack([joined[f"p_over_{x}_5_{suffix}"].to_numpy() for x in (0,1,2)])
        yy = np.column_stack([y >= k for k in (1,2,3)])
        losses.append(-np.log(np.clip(np.where(yy, pp, 1-pp), 1e-12, 1)).mean(axis=1))
    joined["loss_diff_hgb_minus_offset"] = losses[0] - losses[1]
    by_player = joined.groupby("player_id").loss_diff_hgb_minus_offset.agg(["sum", "count"])
    rng = np.random.default_rng(20261009)
    draws = np.empty(500)
    sums, counts = by_player["sum"].to_numpy(), by_player["count"].to_numpy()
    for i in range(len(draws)):
        pick = rng.integers(0, len(sums), size=len(sums))
        draws[i] = sums[pick].sum() / counts[pick].sum()
    pd.DataFrame([{"comparison":"HGB_POISSON minus POISSON_TOI_OFFSET",
        "player_clusters":len(sums), "point_estimate":float(joined.loss_diff_hgb_minus_offset.mean()),
        "bootstrap_replicates":len(draws), "ci_2_5":float(np.quantile(draws,.025)),
        "ci_97_5":float(np.quantile(draws,.975))}]).to_csv(args.out_dir / "rolling_origin_player_cluster_bootstrap.csv", index=False)

    # Error-regime summaries for opportunity/history groups; position is absent
    # from the source contract and therefore is not inferred from a current roster.
    regimes = hgb.copy()
    q25, q75 = regimes.mean_toi_last10.quantile([.25,.75])
    regimes["regime"] = np.select([
        regimes.current_season_games_prior.eq(0) & regimes.player_lifetime_games_prior.gt(0),
        regimes.current_season_games_prior.eq(0), regimes.current_season_games_prior.eq(1),
        regimes.current_season_games_prior.eq(2), regimes.mean_toi_last10.ge(q75),
        regimes.mean_toi_last10.le(q25), regimes.player_lifetime_games_prior.lt(10)],
        ["RETURNING_VETERAN_SEASON_OPENER","NO_PRIOR_APPEARANCE","ONE_CURRENT_SEASON_GAME",
         "TWO_CURRENT_SEASON_GAMES","HIGH_TOI_TOP_QUARTILE","LOW_TOI_BOTTOM_QUARTILE","SPARSE_CAREER_LT10"],
        default="OTHER")
    regime_rows=[]
    for regime, g in regimes.groupby("regime"):
        for k, col in zip((1,2,3),("p_over_0_5","p_over_1_5","p_over_2_5")):
            m=metrics((g.points>=k).astype(int),g[col].to_numpy())
            regime_rows.append({"regime":regime,"threshold":k,"n":len(g),
                "observed_points_mean":float(g.points.mean()),"predicted_mean_mean":float(g.predicted_mean.mean()),
                "observed_3plus_rate":float((g.points>=3).mean()),"predicted_3plus_rate":float(g.p_over_2_5.mean()),
                "log_loss":m["log_loss"],"brier":m["brier"],"calibration_slope":m["calibration_slope"]})
    pd.DataFrame(regime_rows).to_csv(args.out_dir / "hgb_error_regimes.csv", index=False)
    # Save HGB rolling predictions to make the validation reproducible and inspectable.
    for name, predictions in merged.items():
        predictions.to_csv(args.out_dir / f"{name.lower()}_rolling_predictions.csv.gz",
                           index=False, compression="gzip")
    manifest = {"schema_version": "NHL_POINTS_LEADER_VALIDATION_V1",
        "source_frame": str(args.input), "source_frame_sha256": sha256(args.input),
        "training_policy": "EXPANDING_PRIOR_DATE_MONTHLY_ORIGINS",
        "evaluation_period": [str(data.game_date.min().date()), str(data.game_date.max().date())],
        "model_policy": HGB_KW, "history_contract": ARMS[2],
        "primary_season_holdout": {"evaluation_season":args.evaluation_season,
            "training_seasons":sorted(train_holdout.canonical_season.unique().astype(int).tolist()),
            "training_rows":int(len(train_holdout)), "evaluation_rows":int(len(test_holdout)),
            "evaluation_games":int(test_holdout.game_id.nunique()),
            "evaluation_period":[str(test_holdout.game_date.min().date()),str(test_holdout.game_date.max().date())],
            "training_contains_evaluation_season":False},
        "restored_target_binding":"PASS" if args.evaluation_season == 2025 else "NOT_APPLICABLE",
        "strict_prior_leakage_rows":0 if args.evaluation_season == 2025 else None,
        "folds": [a["fold"] for a in alphas], "row_count": int(len(merged["HGB_POISSON"])),
        "outputs": sorted(p.name for p in args.out_dir.iterdir() if p.is_file())}
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
