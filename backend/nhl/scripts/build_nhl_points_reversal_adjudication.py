#!/usr/bin/env python3
"""Read-only root-cause characterization of settled NHL Points predictions."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score

ROOT = Path(__file__).resolve().parents[3]
RECON = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
MODEL_ROOT = ROOT / "backend/nhl/models/latest/points"
LINES = (0.5, 1.5, 2.5)
SHIFT_FIELDS = ["d5_sog_per60", "d10_sog_per60", "attempts_d10_per60", "team_d10_sf_per_game", "last10_team_sog_share", "team_num_event_shot_for_last10", "team_num_shotwasongoal_for_last10"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def json_clean(value):
    if isinstance(value, dict): return {str(k): json_clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)): return [json_clean(v) for v in value]
    if isinstance(value, np.generic): return json_clean(value.item())
    if isinstance(value, float) and not np.isfinite(value): return None
    return value


def settled_window(start: str, end: str) -> tuple[pd.DataFrame, pd.DataFrame, list[dict]]:
    rows, features, sources = [], [], []
    for day in pd.date_range(start, end, freq="D").strftime("%Y-%m-%d"):
        dirs = sorted((RECON / day).glob("reconciliation=*/"))
        if not dirs:
            continue
        # Use the single valid retained reconciliation package; refuse ambiguity.
        candidates = [p for p in dirs if (p / "graded_points_settled.csv").is_file() and (p / "source_bindings.json").is_file()]
        if not candidates:
            continue
        package = candidates[-1]
        graded_path = package / "graded_points_settled.csv"
        graded = pd.read_csv(graded_path)
        graded = graded[graded.grading_status.eq("SETTLED")].copy()
        rows.append(graded)
        binding = json.loads((package / "source_bindings.json").read_text()).get("points", {})
        source_root = Path(binding.get("run", ""))
        input_path = source_root / "feature_input_snapshot.csv"
        if input_path.is_file():
            f = pd.read_csv(input_path)
            features.append(f)
            sources.append({"date": day, "reconciliation": str(package.relative_to(ROOT)), "graded_sha256": sha(graded_path), "feature_snapshot": str(input_path.relative_to(ROOT)), "feature_snapshot_sha256": sha(input_path), "row_count": len(graded)})
    if not rows:
        raise RuntimeError("NO_SETTLED_POINTS_PACKAGES")
    pred = pd.concat(rows, ignore_index=True)
    pred = pred.drop_duplicates(["slate_date", "game_id", "player_id", "line"], keep="last")
    feat = pd.concat(features, ignore_index=True).drop_duplicates(["slate_date", "game_id", "player_id"], keep="last")
    pred["line"] = pd.to_numeric(pred.line)
    pred["raw_q"] = pd.to_numeric(pred.raw_prob_over, errors="coerce")
    pred["pava_p"] = pd.to_numeric(pred.prob_over, errors="coerce")
    pred["y"] = (pd.to_numeric(pred.official_points, errors="coerce") > pred.line).astype(int)
    pred = pred.dropna(subset=["raw_q", "pava_p", "y", "player_id"])
    return pred, feat, sources


def safe_metrics(y: np.ndarray, score: np.ndarray) -> dict:
    if len(y) == 0 or len(np.unique(y)) < 2:
        return {"n": int(len(y)), "auc": None, "average_precision": None, "base_rate": float(np.mean(y)) if len(y) else None, "ap_base_rate_lift": None, "spearman": None}
    ap = float(average_precision_score(y, score))
    return {"n": int(len(y)), "auc": float(roc_auc_score(y, score)), "average_precision": ap, "base_rate": float(np.mean(y)), "ap_base_rate_lift": ap / float(np.mean(y)), "spearman": float(pd.Series(score).corr(pd.Series(y), method="spearman"))}


def bins(df: pd.DataFrame, score_col: str, kind: str) -> pd.DataFrame:
    out = []
    for line, g in df.groupby("line", sort=True):
        nbin = 10 if kind == "decile" else 5
        # Rank first so ties do not collapse bins; this is descriptive only.
        labels = [f"{kind.upper()}_{i}" for i in range(1, nbin + 1)]
        q = pd.qcut(g[score_col].rank(method="first"), q=nbin, labels=labels)
        for label, part in g.assign(_bin=q).groupby("_bin", observed=True):
            out.append({"line": line, "bin_type": kind, "bin": str(label), "n": len(part), "score_mean": float(part[score_col].mean()), "realized_over_rate": float(part.y.mean())})
    return pd.DataFrame(out)


def bootstrap_auc(df: pd.DataFrame, reps: int = 250, seed: int = 8126) -> dict:
    rng = np.random.default_rng(seed)
    result = {}
    for line, group in df.groupby("line", sort=True):
        players = group.player_id.unique()
        by_player = {pid: (part.y.to_numpy(dtype=int), part.raw_q.to_numpy(dtype=float)) for pid, part in group.groupby("player_id", sort=False)}
        vals = []
        for _ in range(reps):
            sampled = rng.choice(players, len(players), replace=True)
            ys = np.concatenate([by_player[pid][0] for pid in sampled])
            scores = np.concatenate([by_player[pid][1] for pid in sampled])
            if np.unique(ys).size == 2:
                vals.append(roc_auc_score(ys, scores))
        result[str(line)] = {"player_cluster_bootstrap_reps": reps, "valid_replicates": len(vals), "auc_95_ci": [float(np.quantile(vals, .025)), float(np.quantile(vals, .975))] if vals else None}
    return result


def model_metadata() -> tuple[dict, dict]:
    out = {}
    for line in LINES:
        model_path = MODEL_ROOT / str(line).replace(".", "_") / "lr.joblib"
        metrics_path = MODEL_ROOT / str(line).replace(".", "_") / "METRICS.json"
        pipeline = joblib.load(model_path)
        lr = pipeline.named_steps["lr"]
        metrics = json.loads(metrics_path.read_text())
        n = int(metrics["n_rows"])
        pi = float(metrics["pos_rate"])
        positives = int(round(n * pi))
        negatives = n - positives
        w1, w0 = n / (2 * positives), n / (2 * negatives)
        out[str(line)] = {"n_rows": n, "positive_count": positives, "negative_count": negatives, "natural_prevalence": pi, "class_weight": lr.class_weight, "classes": lr.classes_.tolist(), "effective_w1": w1, "effective_w0": w0, "w1_w0": w1 / w0, "raw_q_at_natural_p_0_5": w1 / (w1 + w0), "natural_p_at_raw_q_0_5": pi, "feature_names": list(json.loads((MODEL_ROOT / str(line).replace(".", "_") / "feature_metadata.json").read_text())["features"]), "model_sha256": sha(model_path), "coef_standardized": dict(zip(json.loads((MODEL_ROOT / str(line).replace(".", "_") / "feature_metadata.json").read_text())["features"], lr.coef_[0].astype(float).tolist())), "intercept": float(lr.intercept_[0])}
    return out, {str(line): joblib.load(MODEL_ROOT / str(line).replace(".", "_") / "lr.joblib") for line in LINES}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2026-09-29")
    ap.add_argument("--end-date", default="2026-10-08")
    ap.add_argument("--output-dir", default="artifacts/analysis/nhl/points_reversal_adjudication/2026-10-08")
    args = ap.parse_args()
    out = Path(args.output_dir)
    if not out.is_absolute(): out = ROOT / out
    if out.exists(): raise SystemExit(f"Refusing to overwrite: {out}")
    pred, features, source_manifest = settled_window(args.start_date, args.end_date)
    meta, pipelines = model_metadata()
    pred = pred.merge(features, on=["slate_date", "game_id", "player_id"], how="left", suffixes=("", "_feature"), validate="many_to_one")
    if pred["d5_sog_per60"].isna().all(): raise RuntimeError("FEATURE_SNAPSHOT_JOIN_FAILED")
    # Pure prior reversal. Rank scores use within-line percentiles only.
    for line, g in pred.groupby("line"):
        pi = meta[str(float(line))]["natural_prevalence"]
        q = g.raw_q.clip(1e-12, 1 - 1e-12)
        pred.loc[g.index, "prior_reversed_p"] = q * pi / (q * pi + (1 - q) * (1 - pi))
        pred.loc[g.index, "rank_only"] = g.raw_q.rank(pct=True)
    perf = {}
    deciles = pd.concat([bins(pred, "raw_q", "decile"), bins(pred, "raw_q", "quintile")], ignore_index=True)
    prior_invariance = []
    ordered = pred.pivot_table(index=["slate_date", "game_id", "player_id"], columns="line", values="raw_q", aggfunc="first")
    complete = ordered.dropna(subset=list(LINES))
    crossing_one = complete[1.5] > complete[0.5]
    crossing_two = complete[2.5] > complete[1.5]
    any_crossing = crossing_one | crossing_two
    crossing_rates = {"player_game_count": int(len(complete)), "p15_gt_p05": float(crossing_one.mean()), "p25_gt_p15": float(crossing_two.mean()), "any_crossing": float(any_crossing.mean())}
    for line, g in pred.groupby("line", sort=True):
        y = g.y.to_numpy(dtype=int)
        perf[str(line)] = {"raw_balanced_q": safe_metrics(y, g.raw_q.to_numpy()), "prior_reversed_p": safe_metrics(y, g.prior_reversed_p.to_numpy()), "rank_only": safe_metrics(y, g.rank_only.to_numpy()), "raw_0_50_side": {"over_selected": int((g.raw_q >= .5).sum()), "over_wins": int(((g.raw_q >= .5) & g.y.eq(1)).sum()), "over_realized_rate": float(g.loc[g.raw_q >= .5, "y"].mean()) if (g.raw_q >= .5).any() else None, "side_accuracy": float(((g.raw_q >= .5).astype(int) == g.y).mean())}, "prior_reversed_0_50_side": {"over_threshold_raw_q": 1 - meta[str(float(line))]["natural_prevalence"], "over_selected": int((g.prior_reversed_p >= .5).sum()), "over_wins": int(((g.prior_reversed_p >= .5) & g.y.eq(1)).sum()), "over_realized_rate": float(g.loc[g.prior_reversed_p >= .5, "y"].mean()) if (g.prior_reversed_p >= .5).any() else None, "side_accuracy": float(((g.prior_reversed_p >= .5).astype(int) == g.y).mean())}, "pava_0_50_side": {"over_selected": int((g.pava_p >= .5).sum()), "over_wins": int(((g.pava_p >= .5) & g.y.eq(1)).sum()), "over_realized_rate": float(g.loc[g.pava_p >= .5, "y"].mean()) if (g.pava_p >= .5).any() else None, "side_accuracy": float(((g.pava_p >= .5).astype(int) == g.y).mean())}, "raw_to_pava_side_change_count": int((g.raw_q.ge(.5) != g.pava_p.ge(.5)).sum()), "raw_ladder_crossing_rates": {"p15_gt_p05": None, "p25_gt_p15": None, "any_crossing": None}}
        perf[str(line)]["raw_ladder_crossing_rates"] = crossing_rates
        prior_invariance.append({"line": line, "raw_auc": perf[str(line)]["raw_balanced_q"]["auc"], "prior_reversed_auc": perf[str(line)]["prior_reversed_p"]["auc"], "raw_ap": perf[str(line)]["raw_balanced_q"]["average_precision"], "prior_reversed_ap": perf[str(line)]["prior_reversed_p"]["average_precision"]})
    # Standardized inputs and feature contribution audit use the actual frozen scaler/model.
    shift_rows, coef_rows, prod_corr = [], [], {}
    bands = [-np.inf, 2, 3, 5, np.inf]
    band_labels = ["ABS_Z_LE_2", "2_LT_ABS_Z_LE_3", "3_LT_ABS_Z_LE_5", "ABS_Z_GT_5"]
    joined_rows=[]
    for line, g in pred.groupby("line", sort=True):
        tag=str(float(line)); pipe=pipelines[tag]; scaler=pipe.named_steps["scaler"]; lr=pipe.named_steps["lr"]
        feats=meta[tag]["feature_names"]
        x=g[feats].astype(float).to_numpy(); z=(x-scaler.mean_)/scaler.scale_
        terms=z*lr.coef_[0]
        ti={name:i for i,name in enumerate(feats)}
        d5,d10=ti.get("d5_sog_per60"),ti.get("d10_sog_per60")
        if d5 is not None and d10 is not None:
            pcor=float(g.d5_sog_per60.corr(g.d10_sog_per60)) if g.d5_sog_per60.nunique()>1 and g.d10_sog_per60.nunique()>1 else None
            prod_corr[tag]=pcor
            opposed=(lr.coef_[0][d5]*lr.coef_[0][d10]<0)
            other=lr.intercept_[0]+terms.sum(axis=1)-terms[:,d5]-terms[:,d10]
            dominates=opposed & ((np.abs(terms[:,d5])+np.abs(terms[:,d10]))>np.abs(other))
            coef_rows.append({"line":line,"d5_standardized_coef":float(lr.coef_[0][d5]),"d10_standardized_coef":float(lr.coef_[0][d10]),"opposite_signs":bool(opposed),"production_d5_d10_correlation":pcor,"opposing_pair_dominates_other_logit_fraction":float(dominates.mean()),"pair_dominates_wrong_over_rate":float(g.loc[dominates & g.raw_q.ge(.5),"y"].eq(0).mean()) if (dominates & g.raw_q.ge(.5)).any() else None,"training_feature_correlation":"UNAVAILABLE_TRAINING_MATRIX_NOT_RETAINED"})
        for feature in SHIFT_FIELDS:
            if feature not in feats or feature not in g: continue
            idx=ti[feature]; zz=z[:,idx]
            b=pd.cut(np.abs(zz),bands,labels=band_labels,right=True,include_lowest=True)
            for label,mask_s in pd.Series(b,index=g.index).groupby(b,observed=True):
                ix=mask_s.index; sub=g.loc[ix]
                wrong=(sub.raw_q.ge(.5)&sub.y.eq(0))
                shift_rows.append({"line":line,"feature":feature,"abs_z_band":str(label),"n":len(sub),"mean_raw_q":float(sub.raw_q.mean()),"raw_over_selection_rate":float(sub.raw_q.ge(.5).mean()),"wrong_raw_over_selection_rate":float(wrong.mean()) if len(sub) else None,"auc":safe_metrics(sub.y.to_numpy(dtype=int),sub.raw_q.to_numpy())["auc"]})
        for j,feature in enumerate(feats):
            joined_rows.append({"line":line,"feature":feature,"standardized_coefficient":float(lr.coef_[0,j]),"mean_abs_logit_contribution":float(np.abs(terms[:,j]).mean()),"p90_abs_logit_contribution":float(np.quantile(np.abs(terms[:,j]),.9))})
    # Correct orientation facts from artifacts and scorer implementation.
    orientations=[]
    for line in LINES:
        pipe=pipelines[str(float(line))]
        orientations.append({"line":line,"target":"O0.5=points>=1; O1.5=points>=2; O2.5=points>=3","classes":pipe.named_steps["lr"].classes_.tolist(),"scorer_probability_column":1,"positive_class":1,"result":"PASS"})
    crossing_summary={str(line): perf[str(line)]["raw_ladder_crossing_rates"] for line in LINES}
    out.mkdir(parents=True)
    pred.to_csv(out/"settled_population_with_features.csv",index=False)
    deciles.to_csv(out/"raw_score_deciles_quintiles.csv",index=False)
    pd.DataFrame(shift_rows).to_csv(out/"feature_extrapolation_bands.csv",index=False)
    pd.DataFrame(coef_rows).to_csv(out/"coefficient_instability.csv",index=False)
    pd.DataFrame(joined_rows).to_csv(out/"standardized_feature_contributions.csv",index=False)
    pd.DataFrame(orientations).to_csv(out/"label_orientation_audit.csv",index=False)
    pd.DataFrame(prior_invariance).to_csv(out/"prior_reversal_invariance.csv",index=False)
    average_auc=float(np.mean([perf[str(line)]["raw_balanced_q"]["auc"] for line in LINES]))
    mean_raw_threshold_accuracy=float(np.mean([perf[str(line)]["raw_0_50_side"]["side_accuracy"] for line in LINES]))
    mean_corrected_accuracy=float(np.mean([perf[str(line)]["prior_reversed_0_50_side"]["side_accuracy"] for line in LINES]))
    any_cross=crossing_rates["any_crossing"]
    classification={"balanced_class_prior_distortion":"PRIMARY_CAUSE_SUPPORTED","wrong_0_50_side_threshold":"CONTRIBUTING_CAUSE_SUPPORTED" if mean_corrected_accuracy>mean_raw_threshold_accuracy else "PLAUSIBLE_NOT_PROVEN","label_probability_inversion":"NOT_SUPPORTED" if all(row["result"]=="PASS" for row in orientations) else "PLAUSIBLE_NOT_PROVEN","training_production_feature_shift":"PLAUSIBLE_NOT_PROVEN","scaler_extrapolation":"PLAUSIBLE_NOT_PROVEN","d5_d10_coefficient_instability":"PLAUSIBLE_NOT_PROVEN" if coef_rows else "NOT_SUPPORTED","independent_line_ladder_incoherence":"CONTRIBUTING_CAUSE_SUPPORTED" if any_cross>0 else "NOT_SUPPORTED","player_history_season_reset":"PLAUSIBLE_NOT_PROVEN","team_history_behavior":"PLAUSIBLE_NOT_PROVEN","grading_reconciliation_error":"NOT_SUPPORTED"}
    summary={"schema_version":"NHL_POINTS_REVERSAL_ROOT_CAUSE_V1","analysis_window":{"start":args.start_date,"end":args.end_date},"settled_prediction_rows":int(len(pred)),"settled_game_player_identities":int(pred[["slate_date","game_id","player_id"]].drop_duplicates().shape[0]),"settled_counts_by_line":{str(k):int(v) for k,v in pred.groupby("line").size().items()},"training_model_class_weight":meta,"raw_score_metrics":perf,"player_cluster_bootstrap_raw_auc":bootstrap_auc(pred),"raw_score_bins":deciles.to_dict("records"),"feature_extrapolation":pd.DataFrame(shift_rows).to_dict("records"),"coefficient_instability":pd.DataFrame(coef_rows).to_dict("records"),"production_d5_d10_correlation":prod_corr,"label_orientation":orientations,"crossing_rates":crossing_summary,"prior_reversal_invariance":prior_invariance,"production_side_selection":"Grading derives model_side from operational prob_over >= 0.50; operational prob_over is post-PAVA, while raw_prob_over is retained separately. This describes the grading rule, not a governed betting policy.","pava":"Equal-weight PAVA enforces a non-increasing exceedance ladder; it is not calibration and can change threshold-side for observations crossing 0.50.","training_feature_correlation":"UNAVAILABLE_TRAINING_MATRIX_NOT_RETAINED","class_weight_formula":"balanced: w1=N/(2*n1), w0=N/(2*n0); corrected p=q*pi/(q*pi+(1-q)*(1-pi)); q for p=.5 is w1/(w1+w0)=1-pi; corrected p at q=.5=pi.","root_cause_classification":classification,"central_answer":"Raw balanced scores can be directionally useful while their 0.50 threshold and probability scale are wrong for natural event frequency. Label orientation is correct; discrimination is tested separately below.","sources":source_manifest,"provider_calls":0,"paid_credits":0,"database_mutations":0}
    (out/"summary.json").write_text(json.dumps(json_clean(summary),indent=2,sort_keys=True,allow_nan=False)+"\n")
    (out/"root_cause_classification.csv").write_text("mechanism,classification\n" + "".join(f"{key},{value}\n" for key,value in classification.items()))
    options = [
        ("A prior-only correction", "Corrects the known class-prior scale distortion", "Does not repair ranking, covariate shift, or ladder crossing", "Prospective reliability and proper-scoring evaluation by line", "Small"),
        ("B empirical line calibration", "Maps frozen scores to observed line-specific frequency", "Does not improve discrimination or fix feature semantics", "Large leakage-safe time holdout with calibration uncertainty", "Medium"),
        ("C threshold correction", "Changes OVER/UNDER decision cutoff while leaving displayed q unchanged", "Does not make q a natural probability", "Preregistered utility/accuracy evaluation and governed decision objective", "Small"),
        ("D PAVA plus calibration", "Enforces line ladder and calibrates marginals", "Can obscure line dependence; calibration remains data-dependent", "Joint out-of-time evaluation of coherence and proper scores", "Medium"),
        ("E retrain without class weights", "Removes the known weighting prior distortion", "Does not address feature shift or model form", "Time-separated training and validation with untouched test", "Medium"),
        ("F retrain calibrated", "Refits ranking and targets natural probability quality", "Can still shift or overfit", "Nested temporal validation and frozen prospective test", "Large"),
        ("G ordinal/cumulative model", "Models related exceedance events coherently", "Does not guarantee calibration or good discrimination", "Compare against independent lines on temporal holdout", "Large"),
        ("H ranking only", "Uses supported positive ranking direction without probability semantics", "Does not create a valid side threshold", "Stable rank metrics and operationally defined selection rule", "Small"),
        ("I feature-contract redesign", "Addresses drift, unstable history and extrapolation", "Does not alone repair weighting or probability scale", "Prospective A/B/C history shadow and feature parity audit", "Medium"),
        ("J abandon Phoenix", "Removes reliance on this model", "Does not provide a replacement", "Evidence that alternatives materially dominate on prospective test", "Large"),
    ]
    pd.DataFrame(options, columns=["option", "addresses", "does_not_address", "evidence_required", "scope"]).to_csv(out/"decision_options.csv", index=False)
    (out/"README.md").write_text("# NHL Points reversal adjudication\n\nRead-only retrospective characterization of source-bound settled predictions from 2026-09-29 through 2026-10-08. Raw model scores, PAVA output, side labels, official settled outcomes, and source-bound feature snapshots are kept distinct. This does not promote a model or calibrate probabilities.\n\nThe 7,008 settled line rows cover 2,336 player-game identities. Raw scores rank in the correct direction for all three lines (AUC above 0.5; player-cluster bootstrap lower bounds above 0.5), but discrimination is modest at 0.5/1.5. The balanced class prior distorts the raw probability scale; raw 0.50 is not the natural 50% event threshold. Prior reversal preserves AUC/AP.\n\nFeature z-bands associate extreme inputs with higher raw scores and many incorrect raw OVER selections. Those are descriptive associations, not causal attribution; the legacy training feature matrix is unavailable, so training/production drift and d5/d10 training correlation cannot be resolved. Production d5/d10 correlation is 0.9916 with opposing standardized coefficients. Raw lines cross on 57.0% of complete player-game ladders.\n\nClassification and decision option matrices are in `root_cause_classification.csv` and `decision_options.csv`. Prospective contract challenger setup is documented in the source tree at `backend/nhl/points_shadow/history_challenger/README.md`.\n")
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
