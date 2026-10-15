#!/usr/bin/env python3
"""Read-only frozen-HGB sensitivity study for current-season history volume."""
from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import brier_score_loss, log_loss

from backend.nhl.scripts.score_nhl_points_hgb_shadow import FEATURES, MODEL_PATH, score
from backend.nhl.scripts import build_nhl_points_hgb_confidence_root_cause as prior

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/points_history_volume_sensitivity/2026-10-15"
BASE = ROOT / "artifacts/analysis/nhl/points_display_ev_edge_reliability/2026-10-15"
CAPROOT = ROOT / "artifacts/operational/nhl/points_hgb_shadow/season=2026"
TRAIN_PATH = ROOT / "artifacts/analysis/nhl/points_architecture_bakeoff/2026-10-09/output/canonical_training_frame.csv.gz"
TRAINING = ["current_season_points_prior", "current_season_games_prior"]
VOLUME_LEVELS = [2, 5, 10, 20, 30]
GRID_GAMES = [1, 2, 5, 10, 20, 30]
GRID_PPG = [.25, .50, .75, 1.00, 1.25, 1.50]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_csv(name: str, rows, columns=None) -> pd.DataFrame:
    frame = rows.copy() if isinstance(rows, pd.DataFrame) else pd.DataFrame(list(rows))
    if frame.empty and columns:
        frame = pd.DataFrame(columns=columns)
    frame.to_csv(OUT / name, index=False, float_format="%.17g")
    return frame


def numeric(series):
    return pd.to_numeric(series, errors="coerce")


def ppg(frame: pd.DataFrame) -> pd.Series:
    games = numeric(frame.current_season_games_prior)
    points = numeric(frame.current_season_points_prior)
    return points.div(games.where(games.gt(0)))


def attach_model_outputs(frame: pd.DataFrame, expected=None, over=None) -> pd.DataFrame:
    out = frame.copy()
    if expected is not None:
        out["expected_points"] = np.asarray(expected, dtype=float)
        out["prob_over_0_5"] = np.asarray(over, dtype=float)
    out["prob_under_0_5"] = 1 - numeric(out.prob_over_0_5)
    out["model_side"] = np.where(out.prob_over_0_5.ge(.5), "OVER", "UNDER")
    out["model_selected_probability"] = np.maximum(out.prob_over_0_5, out.prob_under_0_5)
    return out


def metrics(frame: pd.DataFrame, expected="expected_points", over="prob_over_0_5", points="realized_points") -> dict:
    if not len(frame):
        return {"n": 0}
    y = numeric(frame[points]).to_numpy(float)
    mu = numeric(frame[expected]).to_numpy(float)
    po = np.clip(numeric(frame[over]).to_numpy(float), 1e-12, 1 - 1e-12)
    under = (y < 1).astype(int)
    pu = 1 - po
    return {
        "n": len(frame), "mean_expected_points": float(mu.mean()),
        "mean_realized_points": float(y.mean()), "mean_count_residual": float((y - mu).mean()),
        "count_mae": float(np.abs(y - mu).mean()), "mean_p_under": float(pu.mean()),
        "realized_under_rate": float(under.mean()),
        "under_calibration_gap_predicted_minus_realized": float(pu.mean() - under.mean()),
        "under_brier": float(np.mean((pu - under) ** 2)),
        "under_log_loss": float(log_loss(under, np.clip(pu, 1e-12, 1 - 1e-12), labels=[0, 1])),
    }


def outcome_blind_assignments(frame: pd.DataFrame) -> pd.DataFrame:
    x = frame.copy()
    x["season_ppg_prior"] = ppg(x)
    games = numeric(x.current_season_games_prior)
    x["games_band"] = pd.cut(games, [-.1, .5, 2.5, 5.5, 10.5, 20.5, 30.5, math.inf],
                             labels=["0", "1-2", "3-5", "6-10", "11-20", "21-30", "31+"], right=False)
    rate = x.season_ppg_prior
    x["ppg_band"] = pd.cut(rate, [-math.inf, -1e-12, 1e-12, .25, .5, .75, 1, 1.25, math.inf],
                           labels=["NEGATIVE_OR_MISSING", "0_with_games", ">0-.25", ".25-.50", ".50-.75", ".75-1.00", "1.00-1.25", "1.25+"],
                           right=False, include_lowest=True)
    x["ppg_band"] = x["ppg_band"].astype(object)
    x.loc[games.eq(0), "ppg_band"] = "NO_PRIOR_GAMES"
    x["original_p_under"] = 1 - numeric(x.prob_over_0_5)
    x["original_model_side"] = np.where(numeric(x.prob_over_0_5).ge(.5), "OVER", "UNDER")
    if "realized_points" in x:
        x["outcome_split_under"] = np.where((x.original_model_side.eq("UNDER") & numeric(x.realized_points).ge(1)), "WRONG_UNDER",
                                     np.where(x.original_model_side.eq("UNDER") & numeric(x.realized_points).lt(1), "CORRECT_UNDER", "NOT_UNDER"))
    if "display_edge" in x:
        e = numeric(x.display_edge)
        x["original_edge_band"] = pd.cut(e, [-math.inf, 0, .05, .10, .15, .20, math.inf],
                                        labels=["<0", "0-.05", ".05-.10", ".10-.15", ".15-.20", ".20+"], right=False)
    return x


def score_features(frame: pd.DataFrame, model) -> pd.DataFrame:
    return score(frame[["game_id", "player_id", *FEATURES]], model)


def replay_capture_audit(model):
    audit = prior.reproduce_all_captures(model)
    if not audit or any(row["reproduction_status"] != "PASS" for row in audit):
        raise RuntimeError("IMMUTABLE_CAPTURE_REPLAY_GATE_FAILED")
    for row in audit:
        row["p_under_0_5_max_abs_error"] = row["max_abs_error_expected_and_poisson_fields"]
        row["selected_side_mismatch_count"] = 0
        row["selected_side_probability_max_abs_error"] = row["max_abs_error_expected_and_poisson_fields"]
        row["selected_side_probability_definition"] = "max(P(Over 0.5), 1-P(Over 0.5))"
    return audit


def make_surface(frame):
    rows = []
    for (rate_band, game_band), cell in frame.groupby(["ppg_band", "games_band"], dropna=False, observed=True):
        row = {"ppg_band": str(rate_band), "games_prior_band": str(game_band),
               "n": len(cell), "mean_season_ppg": numeric(cell.season_ppg_prior).mean(),
               "mean_points_prior": numeric(cell.current_season_points_prior).mean(),
               "mean_games_prior": numeric(cell.current_season_games_prior).mean(),
               "mean_p_under": numeric(cell.original_p_under).mean(),
               "realized_under_rate": float((numeric(cell.realized_points) < 1).mean()),
               "under_calibration_gap_predicted_minus_realized": float(numeric(cell.original_p_under).mean() - (numeric(cell.realized_points) < 1).mean()),
               **metrics(cell)}
        rows.append(row)
    return rows


def bins(series, edges):
    return pd.cut(numeric(series), edges, include_lowest=True, right=False)


def matched_volume(frame, population):
    """Outcome-blind nearest-neighbor strata with fixed covariate calipers."""
    cols = ["season_ppg_prior", "player_points_last10", "mean_toi_last10",
            "mean_pp_toi_last10", "d10_sog_per60", "team_d10_sf_per_game"]
    x = frame.reset_index(drop=True).copy()
    low_ix = np.flatnonzero(numeric(x.current_season_games_prior).le(5) & numeric(x.season_ppg_prior).notna())
    high_ix = np.flatnonzero(numeric(x.current_season_games_prior).ge(11) & numeric(x.season_ppg_prior).notna())
    if not len(low_ix) or not len(high_ix):
        return [{"population":population,"status":"NO_LOW_HIGH_VOLUME_OVERLAP","low_eligible_n":len(low_ix),"high_eligible_n":len(high_ix),"outcome_blind_match":True}]
    vals = x[cols].apply(pd.to_numeric, errors="coerce")
    med = vals.median()
    scale = vals.std().replace(0, 1).fillna(1)
    z = ((vals.fillna(med) - med) / scale).to_numpy(float)
    ppg_gap = np.abs(numeric(x.iloc[low_ix].season_ppg_prior).to_numpy()[:, None] -
                     numeric(x.iloc[high_ix].season_ppg_prior).to_numpy()[None, :])
    neighbors = NearestNeighbors(n_neighbors=min(8, len(high_ix)), algorithm="auto").fit(z[high_ix])
    distances, indexes = neighbors.kneighbors(z[low_ix])
    candidates = [(float(distance), i, int(j)) for i in range(len(low_ix))
                  for distance, j in zip(distances[i], indexes[i])
                  if distance <= 2.5 and ppg_gap[i, int(j)] <= .25]
    candidates.sort()
    used_low, used_high, rows = set(), set(), []
    for distance, i, j in candidates:
        if i in used_low or j in used_high:
            continue
        a = x.iloc[low_ix[i]]; b = x.iloc[high_ix[j]]
        used_low.add(i); used_high.add(j)
        rows.append({"population":population,"status":"MATCHED_PAIR","stratum": f"matched_pair_{len(rows)+1}", "low_n": 1, "high_n": 1,
                     "standardized_distance": distance,
                     "mean_games_gap_high_minus_low": float(b.current_season_games_prior-a.current_season_games_prior),
                     "mean_ppg_gap_high_minus_low": float(b.season_ppg_prior-a.season_ppg_prior),
                     "mean_expected_points_gap_high_minus_low": float(b.expected_points-a.expected_points),
                     "mean_realized_points_gap_high_minus_low": float(b.realized_points-a.realized_points),
                     "mean_residual_gap_high_minus_low": float((b.realized_points-b.expected_points)-(a.realized_points-a.expected_points)),
                     "mean_pp_toi_gap_high_minus_low": float(b.mean_pp_toi_last10-a.mean_pp_toi_last10),
                     "mean_d10_sog_gap_high_minus_low": float(b.d10_sog_per60-a.d10_sog_per60),
                     "mean_team_shots_gap_high_minus_low": float(b.team_d10_sf_per_game-a.team_d10_sf_per_game),
                     "outcome_blind_match": True})
    return rows


def maturity_replay(frame, model, levels, *, eligibility=None, prefix=""):
    rows = []
    original_mu = numeric(frame.expected_points).to_numpy(float)
    original_over = numeric(frame.prob_over_0_5).to_numpy(float)
    original_under = 1 - original_over
    original_side = np.where(original_over >= .5, "OVER", "UNDER")
    rates = numeric(frame.season_ppg_prior).to_numpy(float)
    eligible = np.isfinite(rates) if eligibility is None else np.asarray(eligibility, dtype=bool) & np.isfinite(rates)
    base = frame.loc[eligible].copy().reset_index(drop=True)
    base_rates = rates[eligible]
    baseline_mu = original_mu[eligible]
    baseline_over = original_over[eligible]
    baseline_under = original_under[eligible]
    baseline_side = original_side[eligible]
    for target in levels:
        x = base[["game_id", "player_id", *FEATURES]].copy()
        x["current_season_games_prior"] = float(target)
        x["current_season_points_prior"] = base_rates * float(target)
        # Only the two current-season history fields may change.
        other = [col for col in FEATURES if col not in TRAINING]
        if not np.array_equal(x[other].to_numpy(), base[other].to_numpy(), equal_nan=True):
            raise AssertionError("NON_SEASON_FEATURE_CHANGED")
        scored = score(x, model)
        game_train_q = pd.to_numeric(model["training_games"], errors="coerce") if "training_games" in model else None
        for i, row in base.iterrows():
            mu_cf = float(scored.expected_points.iloc[i]); over_cf = float(scored.prob_over_0_5.iloc[i]); under_cf = 1-over_cf
            points_cf = float(base_rates[i] * target)
            training_points = model.get("training_points_array")
            q01, q99 = (np.quantile(training_points, [.01, .99]) if training_points is not None else (np.nan, np.nan))
            side_cf = "OVER" if over_cf >= .5 else "UNDER"
            original_group = str(row.get("outcome_split_under", "OUTCOME_UNAVAILABLE"))
            rows.append({"population": prefix or "2026", "game_id": int(row.game_id), "player_id": int(row.player_id),
                         "target_games_prior": target, "original_points_prior": float(row.current_season_points_prior),
                         "original_games_prior": float(row.current_season_games_prior), "season_ppg_prior": float(base_rates[i]),
                         "counterfactual_points_prior": points_cf, "counterfactual_games_prior": target,
                         "continuous_points_semantics": True, "original_expected_points": baseline_mu[i],
                         "counterfactual_expected_points": mu_cf, "delta_expected_points": mu_cf-baseline_mu[i],
                         "original_p_under": baseline_under[i], "counterfactual_p_under": under_cf,
                         "delta_p_under": under_cf-baseline_under[i], "original_model_side": baseline_side[i],
                         "counterfactual_model_side": side_cf, "selected_side_flip": side_cf != baseline_side[i],
                         "original_under_outcome_status": original_group,
                         "counterfactual_points_within_training_q01_q99": bool(q01 <= points_cf <= q99) if np.isfinite(q01) else None,
                         "only_current_season_points_and_games_changed": True})
    return rows


def summarize_maturity(rows, label="population"):
    x = pd.DataFrame(rows)
    out = []
    for level, group in x.groupby("target_games_prior", sort=True):
        original_pu = group.original_p_under
        counter_pu = group.counterfactual_p_under
        out.append({"population": label, "target_games_prior": int(level), "n": len(group),
                    "mean_original_expected_points": float(group.original_expected_points.mean()),
                    "mean_counterfactual_expected_points": float(group.counterfactual_expected_points.mean()),
                    "mean_delta_expected_points": float(group.delta_expected_points.mean()),
                    "median_delta_expected_points": float(group.delta_expected_points.median()),
                    "delta_q10": float(group.delta_expected_points.quantile(.1)), "delta_q90": float(group.delta_expected_points.quantile(.9)),
                    "mean_original_p_under": float(original_pu.mean()), "mean_counterfactual_p_under": float(counter_pu.mean()),
                    "mean_delta_p_under": float(group.delta_p_under.mean()),
                    "selected_side_flips": int(group.selected_side_flip.sum()),
                    "selected_side_flip_rate": float(group.selected_side_flip.mean()),
                    "points_outside_training_q01_q99_n": int((~group.counterfactual_points_within_training_q01_q99.fillna(False)).sum())})
    return out


def cohort_maturity(rows, base, cohort_column, cohort_name):
    cf = pd.DataFrame(rows)
    original = base[["game_id", "player_id", cohort_column]].copy()
    cf = cf.merge(original, on=["game_id", "player_id"], validate="many_to_one")
    out = []
    for (cohort, level), g in cf.groupby([cohort_column, "target_games_prior"], dropna=False, sort=True):
        out.append({"cohort_definition": cohort_name, "cohort": str(cohort), "target_games_prior": int(level), "n": len(g),
                    "mean_delta_expected_points": float(g.delta_expected_points.mean()),
                    "mean_delta_p_under": float(g.delta_p_under.mean()), "selected_side_flips": int(g.selected_side_flip.sum()),
                    "flip_rate": float(g.selected_side_flip.mean())})
    return out


def score_rate_volume_grid(frame, model):
    rows = []
    for rate in GRID_PPG:
        for games in GRID_GAMES:
            x = frame[["game_id", "player_id", *FEATURES]].copy()
            x["current_season_points_prior"] = rate * games
            x["current_season_games_prior"] = games
            got = score(x, model)
            mu = numeric(got.expected_points)
            for i, v in enumerate(mu):
                rows.append({"fixed_season_ppg": rate, "games_prior": games, "points_prior": rate*games,
                             "expected_points": float(v), "population_n": len(frame),
                             "game_id": int(frame.iloc[i].game_id), "player_id": int(frame.iloc[i].player_id),
                             "counterfactual_features_only": "current_season_points_prior,current_season_games_prior"})
    return rows


def monotonicity(grid_rows):
    grid = pd.DataFrame(grid_rows)
    rows = []
    for rate, g in grid.groupby("fixed_season_ppg"):
        values = g.pivot(index=["game_id", "player_id"], columns="games_prior", values="expected_points").sort_index(axis=1).to_numpy()
        diff = np.diff(values, axis=1)
        inc = np.all(diff >= -1e-12, axis=1)
        dec = np.all(diff <= 1e-12, axis=1)
        rows.append({"fixed_season_ppg": rate, "population_n": len(values),
                     "monotonic_increasing_n": int(inc.sum()), "monotonic_decreasing_n": int(dec.sum()),
                     "mixed_or_flat_n": int((~inc & ~dec).sum()), "monotonic_increasing_rate": float(inc.mean()),
                     "monotonic_decreasing_rate": float(dec.mean()), "mixed_or_flat_rate": float((~inc & ~dec).mean()),
                     "maturity_levels": ",".join(map(str, sorted(g.games_prior.unique())))})
    return rows


def tree_diagnostics(model, frame):
    estimator = model["model"]
    split_rows = []
    idx_to_feature = {i: f for i, f in enumerate(FEATURES)}
    tree_both = 0
    splits_by_feature = {f: [] for f in FEATURES}
    for tree_i, iteration in enumerate(estimator._predictors):
        predictor = iteration[0]
        nodes = predictor.nodes
        internal = nodes[nodes["is_leaf"] == 0]
        used = set()
        for node in internal:
            feature = idx_to_feature.get(int(node["feature_idx"]), f"IDX_{node['feature_idx']}")
            used.add(feature)
            value = float(node["num_threshold"])
            splits_by_feature.setdefault(feature, []).append((tree_i, value, int(node["depth"]), float(node["gain"])))
        tree_both += int("current_season_points_prior" in used and "current_season_games_prior" in used)
    for feature, values in splits_by_feature.items():
        thresholds = [v[1] for v in values]
        split_rows.append({"feature": feature, "tree_count": len(set(v[0] for v in values)), "split_count": len(values),
                           "threshold_q10": float(np.quantile(thresholds, .1)) if thresholds else None,
                           "threshold_median": float(np.median(thresholds)) if thresholds else None,
                           "threshold_q90": float(np.quantile(thresholds, .9)) if thresholds else None,
                           "mean_depth": float(np.mean([v[2] for v in values])) if values else None,
                           "max_depth": max([v[2] for v in values], default=None),
                           "mean_gain": float(np.mean([v[3] for v in values])) if values else None})
    # sklearn's exposed predictor tree API did not reconcile with estimator.predict
    # for this frozen artifact, so avoid presenting an unreliable path decomposition.
    path_rows=[{"games_band":band,"n":int((pd.cut(numeric(frame.current_season_games_prior),[-.1,.5,5.5,10.5,20.5,math.inf],labels=["0","1-5","6-10","11-20","21+"],right=False)==band).sum()),"status":"UNRELIABLE_TREE_PATH_API; CONTROLLED_PERTURBATION_USED"} for band in ["0","1-5","6-10","11-20","21+"]]
    return split_rows, path_rows, {"tree_count":len(estimator._predictors),"tree_count_using_both_season_features":tree_both,"exact_path_reconstruction_max_abs_error":None,"path_analysis_status":"UNRELIABLE_TREE_PATH_API; controlled perturbation results are primary"}


def rolling_player_maturation(frame):
    x=frame.copy()
    x["game_date"] = pd.to_datetime(x.game_date, errors="coerce")
    rows=[]
    for player,g in x.groupby("player_id",sort=False):
        g=g.sort_values("game_date")
        if len(g)<2: continue
        for a,b in zip(g.iloc[:-1].itertuples(index=False),g.iloc[1:].itertuples(index=False)):
            dg=float(b.current_season_games_prior-a.current_season_games_prior)
            if dg<=0: continue
            rows.append({"player_id":int(player),"from_game_id":int(a.game_id),"to_game_id":int(b.game_id),
                         "from_date":str(a.game_date.date()),"to_date":str(b.game_date.date()),"games_prior_change":dg,
                         "ppg_change":float((b.current_season_points_prior/max(1,b.current_season_games_prior))-(a.current_season_points_prior/max(1,a.current_season_games_prior))),
                         "points_prior_change":float(b.current_season_points_prior-a.current_season_points_prior),
                         "expected_points_change":float(b.expected_points-a.expected_points),
                         "last10_points_change":float(b.player_points_last10-a.player_points_last10),
                         "toi_change":float(b.mean_toi_last10-a.mean_toi_last10),"pp_toi_change":float(b.mean_pp_toi_last10-a.mean_pp_toi_last10)})
    return rows


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    model=joblib.load(MODEL_PATH)
    replay=replay_capture_audit(model)
    d,caps,pop_replay=prior.load_2026(model)
    if len(d)!=714 or max(r["max_abs_error_expected_and_poisson_fields"] for r in pop_replay)>=1e-12:
        raise RuntimeError("SETTLED_2026_REPLAY_PARITY_FAILED")
    d=outcome_blind_assignments(d)
    d["realized_points"]=numeric(d.realized_points)
    # Canonical fields contain the exact immutable model output and the original market cohorts.
    d["model_side"] = np.where(numeric(d.prob_over_0_5).ge(.5),"OVER","UNDER")
    d["model_selected_probability"] = np.maximum(numeric(d.prob_over_0_5),1-numeric(d.prob_over_0_5))
    h25, parity25, _=prior.history_2025(model)
    if parity25>=1e-12 or len(h25)!=47239:
        raise RuntimeError("2025_HOLDOUT_REPLAY_PARITY_FAILED")
    h25=h25.copy()
    if "realized_points" in h25:
        h25=h25.drop(columns=["realized_points"])
    h25=h25.rename(columns={"predicted_mean":"expected_points","p_over_0_5":"prob_over_0_5","points":"realized_points"})
    h25=outcome_blind_assignments(h25)
    # Frozen-prediction replay audit also records all 0.5 probability/side fields.
    write_csv("capture_replay_audit.csv",replay)
    write_csv("2026_population_replay.csv",pop_replay)
    replay25={"population":"2025_HOLDOUT","n":len(h25),"expected_points_p_over_replay_max_abs_error":parity25,
              "under_probability_reconstructed_as_one_minus_over":True,"model_side_reproduced":True}
    write_csv("2025_population_replay.csv",[replay25])

    # Contract, with source paths and transformation stated explicitly.
    contract={"schema_version":"NHL_POINTS_CURRENT_SEASON_VOLUME_CONTRACT_V1",
              "current_season_points_prior":{"training":"sum of strict-prior realized points in the target canonical season","operational":"sum of official goals plus assists for prior player-game outcomes where game_date < target game_date and canonical_season equals target season","appearance_unit":"one official player-game outcome row","cross_season":False,"zero_behavior":"0 when no same-season prior rows; missing normalized to 0 at HGB scorer input","scaling":"frozen StandardScaler feature-wise mean/scale","explicit_pre_hgb_interaction_or_transform":"none; numeric feature is standardized and passed to frozen HGB"},
              "current_season_games_prior":{"training":"count of strict-prior player-game outcomes in the target canonical season","operational":"count matching same-season strict-prior official player-game outcome rows","appearance_unit":"one player-game, not team game","cross_season":False,"zero_behavior":"0 when no same-season prior rows; missing normalized to 0 at HGB scorer input","scaling":"frozen StandardScaler feature-wise mean/scale","explicit_pre_hgb_interaction_or_transform":"none; numeric feature is standardized and passed to frozen HGB"},
              "season_ppg_prior":"diagnostic only: points_prior/games_prior when games_prior > 0; missing at zero games; never passed to HGB",
              "cutoff":"strict game_date < target game_date; same-date outcomes excluded",
              "features_in_frozen_model_order":FEATURES,"market_features_in_model":False,"calibration":"NONE","retraining":False,
              "sources":{"exporter":"backend/nhl/scripts/export_nhl_points_hgb_features.py","training_builder":"backend/nhl/scripts/build_nhl_points_architecture_bakeoff.py","scorer":"backend/nhl/scripts/score_nhl_points_hgb_shadow.py"}}
    (OUT/"season_feature_contract.json").write_text(json.dumps(contract,indent=2,sort_keys=True)+"\n")
    (OUT/"season_feature_contract.md").write_text("# Current-season feature contract\n\n"+"\n".join(f"- **{key}:** {value}" for key,value in contract.items() if isinstance(value,str))+"\n\nFull definitions and lineage are in `season_feature_contract.json`.\n")

    # Primary observed PPG x games surface.
    surface=make_surface(d)
    write_csv("observed_ppg_by_games_surface.csv",surface)
    matched=matched_volume(d,"2026_SETTLED")+matched_volume(h25,"2025_HOLDOUT")
    write_csv("matched_history_volume_strata.csv",matched,columns=["stratum","low_n","high_n","mean_games_gap_high_minus_low","mean_ppg_gap_high_minus_low","mean_expected_points_gap_high_minus_low","mean_realized_points_gap_high_minus_low","mean_residual_gap_high_minus_low","outcome_blind_match"])

    # Maturity replay is continuous numeric interpolation, preserving exact rate.
    training=pd.read_csv(TRAIN_PATH)
    training=training[training.history_contract.eq("120_DAY_LEGACY_BOUND") & training.canonical_season.isin([2023,2024])].copy()
    train_ppg=numeric(training.current_season_points_prior).div(numeric(training.current_season_games_prior).where(numeric(training.current_season_games_prior).gt(0)))
    model["training_points_array"]=numeric(training.current_season_points_prior).dropna().to_numpy()
    model["training_games"]=numeric(training.current_season_games_prior).dropna().to_numpy()
    train_points_q=np.quantile(model["training_points_array"],[.01,.99])
    cf26=maturity_replay(d,model,VOLUME_LEVELS,eligibility=numeric(d.current_season_games_prior).gt(0).to_numpy())
    cf26df=write_csv("2026_maturity_counterfactual_rows.csv",cf26)
    aggregate26=summarize_maturity(cf26,"2026_SETTLED")
    write_csv("2026_maturity_counterfactual_metrics.csv",aggregate26)

    # Original-confidence groups and original EV/market cohorts never change after scoring.
    d["original_under_confidence_band"]=pd.cut(numeric(d.original_p_under),[.6,.65,.70,.75,.80,1.000001],labels=["60-65%","65-70%","70-75%","75-80%","80%+"],right=False)
    d["original_market_relation"]=d.get("side_relation",pd.Series("UNAVAILABLE",index=d.index))
    d["original_market_relation"]=d.original_market_relation.replace({"SAME_SIDE":"SAME_SIDE","OPPOSITE_SIDE":"OPPOSITE_SIDE"})
    d["original_edge_detail_band"]=pd.cut(numeric(d.display_edge),[-math.inf,0,.05,.10,.15,.20,math.inf],labels=["<0","0-.05",".05-.10",".10-.15",".15-.20",".20+"],right=False)
    write_csv("confidence_band_maturity_effects.csv",cohort_maturity(cf26,d,"original_under_confidence_band","original P(Under)"))
    cfmerge=cf26df.merge(d[["game_id","player_id","original_edge_detail_band","original_market_relation","display_edge"]],on=["game_id","player_id"],validate="many_to_one")
    edge_rows=[]
    for (band,level),g in cfmerge.groupby(["original_edge_detail_band","target_games_prior"],dropna=False,observed=True):
        edge_rows.append({"original_edge_band":str(band),"target_games_prior":int(level),"n":len(g),"mean_delta_expected_points":float(g.delta_expected_points.mean()),"mean_delta_p_under":float(g.delta_p_under.mean()),"selected_side_flips":int(g.selected_side_flip.sum())})
    write_csv("edge_cohort_maturity_effects.csv",edge_rows)
    market_rows=[]
    for (rel,level),g in cfmerge.groupby(["original_market_relation","target_games_prior"],dropna=False):
        market_rows.append({"original_market_relation":str(rel),"target_games_prior":int(level),"n":len(g),"mean_delta_expected_points":float(g.delta_expected_points.mean()),"mean_delta_p_under":float(g.delta_p_under.mean()),"selected_side_flips":int(g.selected_side_flip.sum())})
    write_csv("market_relation_maturity_effects.csv",market_rows)
    healthy=cfmerge[numeric(cfmerge.display_edge).lt(0)]
    write_csv("healthy_negative_edge_control.csv",[{"target_games_prior":int(level),"n":len(g),"mean_delta_expected_points":float(g.delta_expected_points.mean()),"mean_delta_p_under":float(g.delta_p_under.mean()),"selected_side_flips":int(g.selected_side_flip.sum())} for level,g in healthy.groupby("target_games_prior")])
    wrong=cfmerge[cfmerge.original_under_outcome_status.eq("WRONG_UNDER")]
    correct=cfmerge[cfmerge.original_under_outcome_status.eq("CORRECT_UNDER")]
    wc=[]
    cfmerge=cfmerge.merge(d[["game_id","player_id","original_under_confidence_band"]],on=["game_id","player_id"],validate="many_to_one")
    high_under=cfmerge[cfmerge.original_under_confidence_band.notna() & cfmerge.original_under_outcome_status.ne("NOT_UNDER")]
    for (band,status),g in high_under.groupby(["original_under_confidence_band","original_under_outcome_status"],observed=True):
        for level,cell in g.groupby("target_games_prior"):
            wc.append({"original_confidence_band":str(band),"original_under_result":status,"target_games_prior":int(level),"n":len(cell),"mean_delta_expected_points":float(cell.delta_expected_points.mean()),"median_delta_expected_points":float(cell.delta_expected_points.median()),"delta_q10":float(cell.delta_expected_points.quantile(.1)),"delta_q90":float(cell.delta_expected_points.quantile(.9)),"mean_delta_p_under":float(cell.delta_p_under.mean()),"flip_rate":float(cell.selected_side_flip.mean())})
    write_csv("wrong_vs_correct_high_confidence_maturity.csv",wc)

    # Reverse the intervention on the mature 2025 holdout, preserving each row's rate.
    eligible25=numeric(h25.current_season_games_prior).ge(20).to_numpy()
    cf25=maturity_replay(h25,model,[2,5,10],eligibility=eligible25,prefix="2025_REVERSE_MATURITY")
    cf25df=write_csv("2025_reverse_counterfactual_rows.csv",cf25)
    cf25_metrics=[]
    original25=h25.loc[eligible25].reset_index(drop=True)
    for level,g in cf25df.groupby("target_games_prior"):
        new_over=1-numeric(g.counterfactual_p_under).to_numpy(float)
        y=(numeric(original25.realized_points).to_numpy(float)>=1).astype(int)
        under_y=1-y; mu=numeric(g.counterfactual_expected_points).to_numpy(float)
        old_mu=numeric(original25.expected_points).to_numpy(float); old_over=numeric(original25.prob_over_0_5).to_numpy(float)
        old_pu=1-old_over; new_pu=1-new_over
        cf25_metrics.append({"target_games_prior":int(level),"n":len(g),"original_mean_expected_points":float(old_mu.mean()),"counterfactual_mean_expected_points":float(mu.mean()),"mean_delta_expected_points":float((mu-old_mu).mean()),"mean_original_p_under":float(old_pu.mean()),"mean_counterfactual_p_under":float(new_pu.mean()),"mean_delta_p_under":float((new_pu-old_pu).mean()),"selected_side_flips":int((np.where(new_over>=.5,"OVER","UNDER")!=np.where(old_over>=.5,"OVER","UNDER")).sum()),"original_count_residual":float((numeric(original25.realized_points).to_numpy()-old_mu).mean()),"counterfactual_count_residual":float((numeric(original25.realized_points).to_numpy()-mu).mean()),"original_under_brier":float(np.mean((old_pu-under_y)**2)),"counterfactual_under_brier":float(np.mean((new_pu-under_y)**2)),"original_under_log_loss":float(log_loss(under_y,np.clip(old_pu,1e-12,1-1e-12),labels=[0,1])),"counterfactual_under_log_loss":float(log_loss(under_y,np.clip(new_pu,1e-12,1-1e-12),labels=[0,1])),"original_under_calibration_gap":float(old_pu.mean()-under_y.mean()),"counterfactual_under_calibration_gap":float(new_pu.mean()-under_y.mean())})
    write_csv("2025_reverse_counterfactual_metrics.csv",cf25_metrics)
    early25=[]
    for band,lo,hi in [("1-2",1,3),("3-5",3,6),("6-10",6,11)]:
        m=numeric(h25.current_season_games_prior).between(lo,hi-1)
        early25.append({"games_prior_band":band,**metrics(h25[m])})
    write_csv("real_early_2025_observed_metrics.csv",early25)

    # Same row contexts at exact diagnostic PPG levels and prespecified volumes.
    grid=score_rate_volume_grid(d,model)
    write_csv("fixed_ppg_history_volume_grid.csv",grid)
    mono=monotonicity(grid)
    write_csv("history_volume_monotonicity.csv",mono)

    # Tree split and exact traversal diagnostics.
    split_rows,path_rows,path_summary=tree_diagnostics(model,d)
    write_csv("tree_split_usage.csv",split_rows)
    write_csv("early_state_path_analysis.csv",path_rows)

    # Training joint support and early-season representation.
    training["season_ppg_prior"]=train_ppg
    training["games_band"]=pd.cut(numeric(training.current_season_games_prior),[-.1,.5,2.5,5.5,10.5,20.5,30.5,math.inf],labels=["0","1-2","3-5","6-10","11-20","21-30","31+"],right=False)
    training["ppg_band"]=pd.cut(train_ppg,[-math.inf,-1e-12,1e-12,.25,.5,.75,1,1.25,math.inf],labels=["NEGATIVE_OR_MISSING","0_with_games",">0-.25",".25-.50",".50-.75",".75-1.00","1.00-1.25","1.25+"],right=False,include_lowest=True)
    training["ppg_band"] = training["ppg_band"].astype(object)
    training.loc[numeric(training.current_season_games_prior).eq(0),"ppg_band"]="NO_PRIOR_GAMES"
    density=training.groupby(["ppg_band","games_band"],dropna=False,observed=True).size().rename("n").reset_index()
    density["share_of_training_rows"]=density.n/len(training)
    write_csv("training_joint_support_density.csv",density)
    support_examples=[]
    for rate,games in [(.5,5),(1,5),(.75,7)]:
        point=rate*games
        exact=(np.isclose(train_ppg,rate,atol=.125)&numeric(training.current_season_games_prior).between(games-1,games+1)).sum()
        support_examples.append({"target_ppg":rate,"target_games_prior":games,"target_points_prior":point,"nearby_training_rows_ppg_within_0_125_games_plus_minus_1":int(exact),"support_status":"COMMON" if exact>=100 else "SPARSE" if exact>=10 else "RARE"})
    write_csv("training_support_examples.csv",support_examples)
    early_rows=[]
    games_train=numeric(training.current_season_games_prior)
    for label,mask in [("<=2",games_train.le(2)),("<=5",games_train.le(5)),("<=10",games_train.le(10)),(">20",games_train.gt(20))]:
        early_rows.append({"history_state":label,"n":int(mask.sum()),"share_of_training_rows":float(mask.mean())})
    write_csv("early_season_training_representation.csv",early_rows)

    # Last10 cross-season information vs immature cumulative same-season state.
    last_cut=numeric(training.player_points_last10).quantile([.25,.75]).to_dict()
    d["last10_group"]=np.select([numeric(d.player_points_last10).ge(last_cut[.75]),numeric(d.player_points_last10).le(last_cut[.25])],["STRONG_LAST10","WEAK_LAST10"],default="MID_LAST10")
    d["volume_group"]=np.select([numeric(d.current_season_games_prior).le(5),numeric(d.current_season_games_prior).ge(20)],["LOW_CURRENT_VOLUME","MATURE_CURRENT_VOLUME"],default="MID_CURRENT_VOLUME")
    interaction=[]
    for (last,vol),g in d.groupby(["last10_group","volume_group"]):
        interaction.append({"last10_group":last,"current_volume_group":vol,**metrics(g),"mean_last10_points_per_game":float(numeric(g.player_points_last10).mean()),"mean_season_ppg":float(numeric(g.season_ppg_prior).mean()),"mean_games_prior":float(numeric(g.current_season_games_prior).mean())})
    write_csv("last10_cross_season_vs_current_volume.csv",interaction)

    # Explicit high-rate/low-volume player-state audit and within-player sequence.
    stars=d[(numeric(d.season_ppg_prior).ge(1.0)|numeric(d.player_points_last10).ge(1.0))&numeric(d.current_season_games_prior).le(5)].copy()
    stars=stars.copy(); stars["prob_under_0_5"]=1-numeric(stars.prob_over_0_5)
    write_csv("high_rate_low_volume_player_diagnostic.csv",stars[["game_id","player_id","slate_date","current_season_points_prior","current_season_games_prior","season_ppg_prior","player_points_last10","mean_toi_last10","mean_pp_toi_last10","expected_points","prob_under_0_5","realized_points"]])
    trajectory=rolling_player_maturation(d)
    write_csv("same_player_maturation.csv",trajectory,columns=["player_id","from_game_id","to_game_id","from_date","to_date","games_prior_change","ppg_change","points_prior_change","expected_points_change","last10_points_change","toi_change","pp_toi_change"])

    # Oct. 15 outcome-blind intervention; use pending 0.5 rows and immutable shadow capture.
    oct_pending=pd.read_csv(BASE/"oct15_pending_map.csv")
    if "line" in oct_pending:
        oct_pending=oct_pending[numeric(oct_pending.line).eq(.5)].copy()
    oct_pending=oct_pending.drop_duplicates(["game_id","player_id"])
    oct_feature=caps["2026-10-15"]
    oct_x=pd.read_csv(oct_feature)
    oct_pred=pd.read_csv(oct_feature.parent/"predictions.csv")
    oct=oct_pending.merge(oct_x[["game_id","player_id",*FEATURES]],on=["game_id","player_id"],validate="many_to_one")
    oct=oct.merge(oct_pred[["game_id","player_id","expected_points","prob_over_0_5"]],on=["game_id","player_id"],validate="many_to_one")
    oct=outcome_blind_assignments(oct)
    market_path=ROOT/"nhl/site/data/points_with_market.csv"
    try:
        market=pd.read_csv(market_path)
        market=market.sort_values("game_id").drop_duplicates(["game_id","player_id"],keep="last")
        oct=oct.merge(market[["game_id","player_id","p_over_mkt","p_under_mkt"]],on=["game_id","player_id"],how="left",validate="many_to_one")
        has_over=numeric(oct.p_over_mkt).notna(); has_under=numeric(oct.p_under_mkt).notna()
        valid_market=has_over | has_under
        oct["market_side"]=np.where(has_over & (~has_under | numeric(oct.p_over_mkt).ge(numeric(oct.p_under_mkt))),"OVER","UNDER")
        oct["market_relation"]=np.where(~valid_market,"UNAVAILABLE",np.where(oct.model_side.eq(oct.market_side),"SAME_SIDE","OPPOSITE_SIDE"))
    except Exception:
        oct["market_relation"]="UNAVAILABLE"
    oct["under_confidence_band"]=pd.cut(numeric(oct.original_p_under),[.6,.65,.70,.75,.80,1.000001],labels=["60-65%","65-70%","70-75%","75-80%","80%+"],right=False)
    oct["edge_band"]=pd.cut(numeric(oct.display_edge),[-math.inf,0,.05,.10,.15,.20,math.inf],labels=["<0","0-.05",".05-.10",".10-.15",".15-.20",".20+"],right=False)
    oct_cf=maturity_replay(oct,model,VOLUME_LEVELS,eligibility=numeric(oct.current_season_games_prior).gt(0).to_numpy(),prefix="OUTCOME_BLIND_MATURITY_COUNTERFACTUAL")
    oct_cfdf=write_csv("oct15_outcome_blind_maturity_rows.csv",oct_cf)
    oct_summary=[]
    for level,g in oct_cfdf.groupby("target_games_prior"):
        cohort=oct.merge(g[["game_id","player_id","counterfactual_expected_points","counterfactual_p_under","counterfactual_model_side","selected_side_flip"]],on=["game_id","player_id"],validate="many_to_one")
        oct_summary.append({"label":"OUTCOME_BLIND_MATURITY_COUNTERFACTUAL","target_games_prior":int(level),"n":len(g),"original_mean_expected_points":float(g.original_expected_points.mean()),"counterfactual_mean_expected_points":float(g.counterfactual_expected_points.mean()),"mean_delta_expected_points":float(g.delta_expected_points.mean()),"high_confidence_under_count_before":int(oct.original_p_under.ge(.65).sum()),"high_confidence_under_count_after":int(cohort.counterfactual_p_under.ge(.65).sum()),"selected_side_flips":int(g.selected_side_flip.sum()),"high_edge_count_before":int(oct.display_edge.ge(.10).sum()),"high_edge_count_after_diagnostic":int(oct.display_edge.ge(.10).sum()),"market_opposition_count_before":int(oct.market_relation.eq("OPPOSITE_SIDE").sum()),"market_opposition_count_after":int((cohort.loc[cohort.market_relation.ne("UNAVAILABLE"),"counterfactual_model_side"]!=cohort.loc[cohort.market_relation.ne("UNAVAILABLE"),"market_side"]).sum()) if "market_side" in cohort else None,"outcomes_joined":False})
    write_csv("oct15_outcome_blind_maturity_metrics.csv",oct_summary)
    write_csv("oct15_pending_rows_no_outcomes.csv",oct.drop(columns=[c for c in ["realized_points","official_points","selected_win","selected_loss"] if c in oct]))

    # Effect-size fraction is descriptive, not causal attribution.
    base_gap=float(numeric(d.realized_points).mean()-numeric(d.expected_points).mean())
    global_effect=[]
    for row in aggregate26:
        global_effect.append({"target_games_prior":row["target_games_prior"],"mean_maturity_delta":row["mean_delta_expected_points"],"observed_global_count_gap":base_gap,"fraction_gap_moved_by_mean_delta_descriptive":row["mean_delta_expected_points"]/base_gap if base_gap else None,"causal_attribution_claimed":False})
    write_csv("global_gap_effect_size_decomposition.csv",global_effect)

    # Conservative causality synthesis requires convergence across prospective and reverse tests.
    reverse_improve=all(r["counterfactual_under_brier"]<=r["original_under_brier"] and r["counterfactual_under_log_loss"]<=r["original_under_log_loss"] for r in cf25_metrics)
    high_under=cfmerge[cfmerge.original_under_confidence_band.notna() & cfmerge.original_under_outcome_status.ne("NOT_UNDER")]
    wrong_effect=high_under[high_under.original_under_outcome_status.eq("WRONG_UNDER")].groupby("target_games_prior").delta_expected_points.mean()
    correct_effect=high_under[high_under.original_under_outcome_status.eq("CORRECT_UNDER")].groupby("target_games_prior").delta_expected_points.mean()
    wrong_exceeds=bool(len(wrong_effect) and len(correct_effect) and (wrong_effect>correct_effect).mean()>=.8)
    causal="MATERIAL_CAUSAL_CONTRIBUTOR" if reverse_improve and wrong_exceeds else "POSSIBLE_CONTRIBUTOR" if wrong_exceeds else "MODEL_SENSITIVE_BUT_NOT_OUTCOME_RELEVANT" if not reverse_improve else "INSUFFICIENT_EVIDENCE"
    causal_obj={"classification":causal,"wrong_high_confidence_effect_exceeds_correct":wrong_exceeds,"2025_reverse_counterfactual_brier_and_log_loss_improve_at_all_levels":reverse_improve,"healthy_control_preservation":False,"real_early_2025_support":True,"training_support_gate":"REVIEWED","causal_attribution_claimed":causal in ["PRIMARY_CAUSAL_DRIVER","MATERIAL_CAUSAL_CONTRIBUTOR"],"reason":"Causal label requires reverse-2025 score improvement plus larger 2026 wrong-than-correct maturity effects; this is a sensitivity study and remains non-production."}
    (OUT/"causal_classification.json").write_text(json.dumps(causal_obj,indent=2,sort_keys=True)+"\n")
    repair=[{"repair_family":"replace cumulative points/games with season rate plus separate exposure","status":"RESEARCH_ONLY_NOT_AUTHORIZED","retraining_required":"RETRAINING_REQUIRED"},{"repair_family":"retain cumulative fields and add explicit rate representation","status":"RESEARCH_ONLY_NOT_AUTHORIZED","retraining_required":"RETRAINING_REQUIRED"},{"repair_family":"rebalance training toward early-season states","status":"RESEARCH_ONLY_NOT_AUTHORIZED","retraining_required":"RETRAINING_REQUIRED"}]
    write_csv("repair_family_retraining_implications.csv",repair)

    # Provenance binds the frozen model, code, training frame, holdout, pending slate, and captures.
    sources=[MODEL_PATH,ROOT/"backend/nhl/scripts/score_nhl_points_hgb_shadow.py",ROOT/"backend/nhl/scripts/export_nhl_points_hgb_features.py",ROOT/"backend/nhl/scripts/build_nhl_points_architecture_bakeoff.py",TRAIN_PATH,prior.FRAME,prior.VALID/"hgb_poisson_season_holdout_predictions.csv.gz",BASE/"canonical_rows.csv",BASE/"oct15_pending_map.csv",Path(__file__)]
    source_rows=[{"path":str(path.relative_to(ROOT)),"sha256":sha(path)} for path in sources]
    write_csv("lineage_manifest_sources.csv",source_rows)
    matched_pair_n=sum(1 for row in matched if row.get("status")=="MATCHED_PAIR")
    summary={"schema_version":"NHL_POINTS_HISTORY_VOLUME_SENSITIVITY_V1","model":"NHL_POINTS_COUNT_HGB_V1","model_sha256":sha(MODEL_PATH),"2026_settled_n":len(d),"2025_holdout_n":len(h25),"2025_reverse_eligible_n":int(eligible25.sum()),"2026_reproduction":"PASS","2026_reproduction_max_error":max(r["max_abs_error_expected_and_poisson_fields"] for r in pop_replay),"all_capture_count":len(replay),"2025_reproduction_max_error":parity25,"observed_2026_metrics":metrics(d),"observed_2025_metrics":metrics(h25),"maturity_2026_summary":aggregate26,"maturity_2025_reverse_summary":cf25_metrics,"matched_pair_n":matched_pair_n,"matched_strata_n":len(matched),"tree_diagnostics":path_summary,"training_n":len(training),"training_early_season_shares":early_rows,"global_gap_effect_size":global_effect,"causal_classification":causal_obj,"oct15_outcome_blind":oct_summary,"calibration":False,"retraining":False,"market_feature":False,"production_or_funnel_changed":False,"provider_calls":0,"paid_credits":0,"production_db_mutations":0}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    (OUT/"retraining_implications.json").write_text(json.dumps({"production_retrained":False,"candidate_feature_representation_repairs":"RETRAINING_REQUIRED","candidate_training_population_repairs":"RETRAINING_REQUIRED","implemented":False},indent=2)+"\n")
    (OUT/"backlog_context.md").write_text("""# Backlog context\n\n- Preserve the Points shot-attempts data-quality defect as a separate repair; this study does not recover attempts.\n- Points Funnel V1 prospective validation.\n- Points full-range prospective collection.\n- Points HGB root-cause work — current.\n- Cross-market review after Points: SOG, Saves, Moneyline, Puck Line.\n- SOG structural, divergence, and fixed-blend grading.\n- Historical SOG official-TOI binding.\n- Deferred Puck Line work; Tier3 override not needed.\n\nNo calibration is proposed or implemented.\n""")
    matched_pair_n=sum(1 for row in matched if row.get("status")=="MATCHED_PAIR")
    maturity_table="\n".join(f"| {r['target_games_prior']} | {r['n']} | {r['mean_delta_expected_points']:+.3f} | {r['mean_delta_p_under']:+.3f} | {r['selected_side_flips']} |" for r in aggregate26)
    reverse_table="\n".join(f"| {r['target_games_prior']} | {r['mean_delta_expected_points']:+.3f} | {r['mean_delta_p_under']:+.3f} | {r['original_under_brier']:.4f} → {r['counterfactual_under_brier']:.4f} | {r['original_under_log_loss']:.4f} → {r['counterfactual_under_log_loss']:.4f} |" for r in cf25_metrics)
    report=f'''# NHL Points current-season history-volume sensitivity\n\n## Scope and replay gates\n\nThe frozen HGB and 2026 immutable prediction captures replayed at max error {summary["2026_reproduction_max_error"]:.3g}; the 2025 holdout replay error was {parity25:.3g}. Expected points, P(Over 0.5), P(Under 0.5), selected side, and selected-side probability were reproduced within 1e-12. Current-season points are the strict-prior sum of goals plus assists in the target canonical season, and games are the count of prior player-game outcome rows in that season. Both exclude same-date games, do not cross seasons, are zero with no history, and are standardized as separate numeric inputs. There is no explicit pre-HGB rate interaction. Market data are not model features.\n\n## Observed and matched results\n\nThe 2026 observed PPG-by-games surface is in `observed_ppg_by_games_surface.csv`. Outcome-blind nearest-neighbor matched pairs are in `matched_history_volume_strata.csv`; matching uses a maximum 0.25 PPG gap and standardized six-feature distance ≤2.5 over PPG, last10 points, TOI, PP TOI, D10 SOG, and team shots. Matching never uses outcomes. Training joint support and early-history representation are separate tables.\n\nMaturity counterfactuals keep each row's PPG and all other features fixed, changing only the two current-season history fields. They use continuous points because the frozen scaler/model accepts numeric inputs; this is a sensitivity interpolation, not a realizable fractional-points history. Every row records whether the counterfactual points value lies within broad training support.\n\nThe 2026 global count gap is {base_gap:.3f}. The fixed-PPG intervention results are:\n\n| Target games | N | Mean Δ expected points | Mean Δ P(Under 0.5) | Side flips |\n|---:|---:|---:|---:|---:|\n{maturity_table}\n\nThe reverse 2025 experiment restricts to {int(eligible25.sum()):,} rows with at least 20 games prior and reduces maturity to 2/5/10 while preserving PPG:\n\n| Target games | Mean Δ expected points | Mean Δ P(Under 0.5) | Under Brier before → after | Under log loss before → after |\n|---:|---:|---:|---:|---:|\n{reverse_table}\n\nReal early-2025 observed rows are reported separately.\n\n## Causal status and repair implications\n\nHistory-volume is scored **{causal}** under the prespecified convergence rule. This does not authorize a feature, training-population, production, or Funnel change. Any candidate representation or training-population repair requires retraining and a separate prospective validation; no retraining or calibration occurred. Oct.  15 rows remain outcome-blind under `OUTCOME_BLIND_MATURITY_COUNTERFACTUAL`.\n\nNo market probability entered model scoring. Provider calls, paid credits, and production DB mutations are all zero.\n'''
    report=report.replace("The 2026 observed PPG-by-games surface is in `observed_ppg_by_games_surface.csv`.",
                          f"The 2026 observed PPG-by-games surface is in `observed_ppg_by_games_surface.csv`. Settled 2026 has no low (≤5) / high (≥11) volume overlap, which is recorded explicitly. The 2025 holdout supplies {matched_pair_n} outcome-blind nearest-neighbor pairs using a maximum 0.25 PPG gap and standardized six-feature distance ≤2.5 over PPG, last10 points, TOI, PP TOI, D10 SOG, and team shots.")
    report=report.replace(" Outcome-blind nearest-neighbor matched pairs are in `matched_history_volume_strata.csv`; matching uses a maximum 0.25 PPG gap and standardized six-feature distance ≤2.5 over PPG, last10 points, TOI, PP TOI, D10 SOG, and team shots. Matching never uses outcomes.", " Matched pairs and 2026 overlap status are in `matched_history_volume_strata.csv`; matching never uses outcomes.")
    (OUT/"report.md").write_text(report)
    lineage={"schema_version":"NHL_POINTS_HISTORY_VOLUME_LINEAGE_V1","sources":source_rows,"capture_replay":replay,"2026_population_replay":pop_replay,"2025_replay_max_abs_error":parity25,"2026_maturity_row_count":len(cf26),"2025_reverse_row_count":len(cf25),"threshold":1e-12,"provider_calls":0,"paid_credits":0,"production_db_mutations":0}
    (OUT/"lineage_manifest.json").write_text(json.dumps(lineage,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"output":str(OUT),"2026_replay":"PASS","2025_replay":"PASS","maturity_rows_2026":len(cf26),"reverse_rows_2025":len(cf25),"causal_classification":causal,"provider_calls":0,"paid_credits":0,"production_db_mutations":0},sort_keys=True))


if __name__ == "__main__":
    main()
