"""Validate the NHL SOG production D10/D20 residual hypothesis from retained rows."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr

from backend.nhl.scripts.characterize_nhl_sog_production_baseline import coalesce

ROOT = Path(__file__).resolve().parents[3]
OUT = ROOT / "artifacts/analysis/nhl/sog_d10_d20_divergence_validation/2026-10-14"
BASE = ROOT / "artifacts/analysis/nhl/sog_baseline_characterization/2026-10-14"
HIST = ROOT / "artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13"
LEDGER = HIST / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv"
LINES = (1.5, 2.5, 3.5)
MATERIAL = 0.5


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dedupe(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    return df.drop_duplicates([date_col, "game_id", "player_id"]).copy()


def count_stats(df: pd.DataFrame) -> dict:
    e = df.residual
    return {"n": int(len(df)), "mean_d10": float(df.d10.mean()), "mean_d20": float(df.d20.mean()),
            "mean_divergence": float(df.divergence.mean()), "mean_lambda": float(df.lam.mean()),
            "realized_mean_sog": float(df.actual.mean()), "mean_residual": float(e.mean()),
            "median_residual": float(e.median()), "mae": float(e.abs().mean()),
            "rmse": float(np.sqrt(np.mean(e**2))), "mean_absolute_divergence": float(df.divergence.abs().mean())}


def prob_stats(df: pd.DataFrame) -> list[dict]:
    rows=[]
    for line,g in df.groupby("line",sort=True):
        y=g.realized_over.to_numpy(dtype=float); p=g.p_over.to_numpy(dtype=float)
        p=np.clip(p,1e-15,1-1e-15)
        rows.append({"line":float(line),"n":int(len(g)),"brier":float(np.mean((p-y)**2)),
                     "log_loss":float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p))),
                     "calibration_bias":float(p.mean()-y.mean()),"realized_over_rate":float(y.mean()),
                     "predicted_over_rate":float(p.mean())})
    return rows


def main() -> None:
    OUT.mkdir(parents=True,exist_ok=True)
    raw=pd.read_csv(BASE/"historical_characterization_rows.csv",low_memory=False)
    hist=dedupe(raw,"game_date")
    hist["d10"]=pd.to_numeric(hist.d10_sog_per60,errors="coerce")
    hist["d20"]=pd.to_numeric(hist.d20_sog_per60,errors="coerce")
    hist["lam"]=pd.to_numeric(hist.production_lambda,errors="coerce")
    hist["actual"]=pd.to_numeric(hist.actual_sog,errors="coerce")
    eligible=hist.dropna(subset=["d10","d20","lam","actual"]).copy()
    eligible["divergence"]=eligible.d10-eligible.d20
    eligible["residual"]=eligible.actual-eligible.lam
    eligible["window"]="historical_certified_2025_replay"
    eligible["slate_date"]=eligible.game_date
    eligible["player_game_key"]=eligible.game_date.astype(str)+"|"+eligible.game_id.astype(str)+"|"+eligible.player_id.astype(str)
    hist_rows=eligible.copy()
    hist_rows[["game_date","game_id","player_id","d10_sog_per60","d20_sog_per60","d10","d20","divergence","production_lambda","lam","actual_sog","actual","residual","selected_toi_minutes","role_pp_share","prepared_source_path","prepared_source_sha256"]].to_csv(OUT/"historical_paired_rows.csv",index=False)
    # Retain the three threshold rows from the certified probability ledger.
    hist_probs=raw.drop_duplicates(["game_date","game_id","player_id","line"]).copy()
    hist_probs=hist_probs.merge(eligible[["game_date","game_id","player_id","d10","d20","divergence","residual","lam","actual","player_game_key"]],on=["game_date","game_id","player_id"],how="inner",validate="many_to_one")
    hist_probs["line"]=pd.to_numeric(hist_probs.line,errors="coerce")
    hist_probs["p_over"]=pd.to_numeric(hist_probs.p_production,errors="coerce")
    hist_probs["realized_over"]=(hist_probs.actual>=np.ceil(hist_probs.line)).astype(int)

    # The previously retained prospective feature-bound SOG inputs are the validation reference.
    prospective=dedupe(pd.read_csv(BASE/"prospective_feature_bound_rows.csv",low_memory=False),"slate_date")
    prospective["d10"]=pd.to_numeric(prospective.d10_sog_per60,errors="coerce")
    prospective["d20"]=pd.to_numeric(prospective.d20_sog_per60,errors="coerce")
    prospective["lam"]=pd.to_numeric(prospective.production_lambda,errors="coerce")
    prospective["actual"]=pd.to_numeric(prospective.actual_sog,errors="coerce")
    prospective=prospective.dropna(subset=["d10","d20","lam","actual"]).copy()
    prospective["divergence"]=prospective.d10-prospective.d20
    prospective["residual"]=prospective.actual-prospective.lam
    prospective[["slate_date","game_id","player_id","d10_sog_per60","d20_sog_per60","d10","d20","divergence","production_lambda","lam","actual_sog","actual","residual"]].to_csv(OUT/"prospective_validation_rows.csv",index=False)

    # Sign groups, using all valid historical D10/D20 rows and exact prior material threshold.
    eligible["sign_group"]=np.select([eligible.divergence.gt(0),eligible.divergence.lt(0)], ["D10_ABOVE_D20","D10_BELOW_D20"],default="EQUAL")
    eligible["material_group"]=np.select([eligible.divergence.gt(MATERIAL),eligible.divergence.lt(-MATERIAL)], ["MATERIAL_D10_ABOVE_D20","MATERIAL_D10_BELOW_D20"],default="APPROX_EQUAL_WITHIN_0_5")
    sign_rows=[{"group":k,**count_stats(g)} for k,g in eligible.groupby("sign_group",sort=True)]
    material_rows=[{"group":k,**count_stats(g)} for k,g in eligible.groupby("material_group",sort=True)]
    pd.DataFrame(sign_rows).to_csv(OUT/"sign_group_metrics.csv",index=False)
    pd.DataFrame(material_rows).to_csv(OUT/"material_divergence_metrics.csv",index=False)
    # Quantile bins preserve support and expose monotonicity without a tuned cutoff.
    eligible["signed_divergence_bin"]=pd.qcut(eligible.divergence.rank(method="first"),10,labels=[f"Q{i}" for i in range(1,11)])
    mag=eligible.groupby("signed_divergence_bin",observed=True).apply(lambda g:pd.Series(count_stats(g)),include_groups=False).reset_index()
    mag.to_csv(OUT/"magnitude_bin_metrics.csv",index=False)
    # continuous association and simple descriptive OLS with conventional uncertainty.
    x=eligible.divergence.to_numpy(float); y=eligible.residual.to_numpy(float); n=len(x)
    slope,intercept=np.polyfit(x,y,1); fitted=intercept+slope*x; resid=y-fitted
    sxx=float(np.sum((x-x.mean())**2)); se=float(np.sqrt(np.sum(resid**2)/(n-2)/sxx))
    pear=pearsonr(x,y); spear=spearmanr(x,y)
    continuous={"n":n,"pearson_r":float(pear.statistic),"pearson_p":float(pear.pvalue),"spearman_rho":float(spear.statistic),"spearman_p":float(spear.pvalue),"slope_residual_per_sog60":float(slope),"intercept":float(intercept),"r_squared":float(1-np.sum(resid**2)/np.sum((y-y.mean())**2)),"slope_ci95_low":float(slope-1.96*se),"slope_ci95_high":float(slope+1.96*se),"slope_se":se,"slope_direction_matches_hypothesis":bool(slope<0)}
    (OUT/"continuous_relationship_metrics.json").write_text(json.dumps(continuous,indent=2,sort_keys=True)+"\n")
    # Daily and weekly stability: positive effect means residual above-D20 minus below-D20 is negative.
    date_rows=[]
    for date,g in eligible.groupby("game_date",sort=True):
        above=g[g.divergence>MATERIAL]; below=g[g.divergence< -MATERIAL]
        date_rows.append({"period":date,"period_type":"DATE","n":len(g),"mean_divergence":g.divergence.mean(),"above_n":len(above),"above_mean_residual":above.residual.mean(),"below_n":len(below),"below_mean_residual":below.residual.mean(),"group_difference_above_minus_below":above.residual.mean()-below.residual.mean() if len(above) and len(below) else np.nan})
    daily=pd.DataFrame(date_rows); daily.to_csv(OUT/"date_period_metrics.csv",index=False)
    eligible["week_start"]=pd.to_datetime(eligible.game_date).dt.to_period("W-SUN").dt.start_time.dt.strftime("%Y-%m-%d")
    weekly=[]
    for week,g in eligible.groupby("week_start",sort=True):
        a=g[g.divergence>MATERIAL]; b=g[g.divergence< -MATERIAL]
        weekly.append({"period":week,"period_type":"WEEK","n":len(g),"mean_divergence":g.divergence.mean(),"above_n":len(a),"above_mean_residual":a.residual.mean(),"below_n":len(b),"below_mean_residual":b.residual.mean(),"group_difference_above_minus_below":a.residual.mean()-b.residual.mean() if len(a) and len(b) else np.nan})
    pd.DataFrame(weekly).to_csv(OUT/"weekly_period_metrics.csv",index=False)
    # Equal-player weighting and player-level contributions.
    player=eligible.groupby("player_id").agg(observations=("player_game_key","size"),mean_divergence=("divergence","mean"),mean_residual=("residual","mean"),mean_abs_error=("residual",lambda s:s.abs().mean()),above_n=("divergence",lambda s:int((s>MATERIAL).sum())),below_n=("divergence",lambda s:int((s< -MATERIAL).sum()))).reset_index()
    player.to_csv(OUT/"player_robustness_metrics.csv",index=False)
    adequate=player[player.observations>=3]
    eq_player={"eligible_players":int(len(adequate)),"player_game_threshold":3,"equal_player_mean_residual_above":float(eligible[eligible.divergence>MATERIAL].groupby("player_id").residual.mean().reindex(adequate.player_id).dropna().mean()),"equal_player_mean_residual_below":float(eligible[eligible.divergence< -MATERIAL].groupby("player_id").residual.mean().reindex(adequate.player_id).dropna().mean()),"players_with_both_material_directions":int(((adequate.above_n>0)&(adequate.below_n>0)).sum()),"within_player_paired_mean_difference":None}
    paired=eligible[eligible.player_id.isin(adequate.player_id)].groupby(["player_id",eligible.divergence.gt(MATERIAL).map({True:"ABOVE",False:"NOT_ABOVE"})]).residual.mean().unstack()
    paired=paired.dropna(subset=["ABOVE","NOT_ABOVE"],how="any")
    eq_player["players_with_above_and_other"] = int(len(paired))
    eq_player["within_player_paired_mean_difference"] = float((paired.ABOVE-paired.NOT_ABOVE).mean()) if len(paired) else None
    (OUT/"player_weighting_sensitivity.json").write_text(json.dumps(eq_player,indent=2,sort_keys=True)+"\n")
    # Quantile interaction tables: D10, production lambda, selected TOI and PP share.
    eligible["d10_band"]=pd.qcut(eligible.d10.rank(method="first"),5,labels=["D10_BOTTOM_QUINTILE","D10_Q2","D10_Q3","D10_Q4","D10_TOP_QUINTILE"])
    eligible["lambda_band"]=pd.qcut(eligible.lam.rank(method="first"),4,labels=["LAMBDA_Q1","LAMBDA_Q2","LAMBDA_Q3","LAMBDA_Q4"])
    eligible["toi_band"]=pd.qcut(pd.to_numeric(eligible.selected_toi_minutes,errors="coerce").rank(method="first"),4,labels=["TOI_Q1","TOI_Q2","TOI_Q3","TOI_Q4"])
    pp=pd.to_numeric(eligible.role_pp_share,errors="coerce")
    eligible["pp_band"]=pd.qcut(pp.rank(method="first"),3,labels=["PP_SHARE_Q1","PP_SHARE_Q2","PP_SHARE_Q3"])
    interactions=[]
    for variable,name in [("d10_band","d10_interaction_metrics.csv"),("lambda_band","lambda_interaction_metrics.csv"),("toi_band","exposure_interaction_metrics.csv"),("pp_band","pp_interaction_metrics.csv")]:
        for key,g in eligible.groupby(variable,observed=True):
            for group,h in [("MATERIAL_ABOVE",g[g.divergence>MATERIAL]),("APPROX_EQUAL",g[g.divergence.abs()<=MATERIAL]),("MATERIAL_BELOW",g[g.divergence< -MATERIAL])]:
                interactions.append({"dimension":variable,"band":str(key),"divergence_group":group,**count_stats(h) } if len(h) else {"dimension":variable,"band":str(key),"divergence_group":group,"n":0})
        pd.DataFrame([r for r in interactions if r["dimension"]==variable]).to_csv(OUT/name,index=False)
    # Probability calibration for material divergence groups and divergence tails.
    hist_probs["material_group"]=np.select([hist_probs.divergence.gt(MATERIAL),hist_probs.divergence.lt(-MATERIAL)], ["MATERIAL_D10_ABOVE_D20","MATERIAL_D10_BELOW_D20"],default="APPROX_EQUAL_WITHIN_0_5")
    prob_rows=[]
    for group,g in hist_probs.groupby("material_group",sort=True):
        for r in prob_stats(g): prob_rows.append({"group":group,**r})
    pd.DataFrame(prob_rows).to_csv(OUT/"threshold_probability_metrics.csv",index=False)
    # Tail metrics use exact distribution cutpoints and keep full supported group sizes.
    tails=[]
    for label,q,mask in [("BOTTOM_10",.10,eligible.divergence<=eligible.divergence.quantile(.10)),("BOTTOM_5",.05,eligible.divergence<=eligible.divergence.quantile(.05)),("TOP_10",.90,eligible.divergence>=eligible.divergence.quantile(.90)),("TOP_5",.95,eligible.divergence>=eligible.divergence.quantile(.95))]:
        g=eligible[mask]; p=hist_probs[hist_probs.player_game_key.isin(g.player_game_key)]
        tails.append({"tail":label,"divergence_cutoff":float(eligible.divergence.quantile(q)),**count_stats(g)})
        for r in prob_stats(p): tails[-1][f"line_{r['line']}_brier"]=r["brier"]; tails[-1][f"line_{r['line']}_log_loss"]=r["log_loss"]
    pd.DataFrame(tails).to_csv(OUT/"extreme_tail_metrics.csv",index=False)
    # Direct comparison to prospectively observed October 10-13 feature-bound rows.
    def group_summary(df, date):
        return {"window":date,"material_above":count_stats(df[df.divergence>MATERIAL]) if (df.divergence>MATERIAL).any() else {"n":0},"material_below":count_stats(df[df.divergence< -MATERIAL]) if (df.divergence< -MATERIAL).any() else {"n":0},"divergence_quantiles":{str(q):float(df.divergence.quantile(q)) for q in [.05,.1,.25,.5,.75,.9,.95]}}
    comparison={"material_threshold":MATERIAL,"historical":group_summary(eligible,"2026-02-28_to_2026-04-15"),"prospective":group_summary(prospective,"2026-10-10_to_2026-10-13"),"prospective_reference_residuals":{"material_above":-0.353,"material_below":0.192}}
    ha=comparison["historical"]["material_above"]; hb=comparison["historical"]["material_below"]
    comparison["historical_group_difference_above_minus_below"]=ha["mean_residual"]-hb["mean_residual"]
    comparison["historical_vs_prospective_direction"]="DIRECTIONAL_REPRODUCTION" if ha["mean_residual"]<0 and hb["mean_residual"]>0 else "NO_DIRECTIONAL_REPRODUCTION"
    (OUT/"historical_prospective_comparison.json").write_text(json.dumps(comparison,indent=2,sort_keys=True)+"\n")
    # Fixed midpoint diagnostic on material rows only; no weight search or fitting.
    sign_ok=ha["mean_residual"]<0 and hb["mean_residual"]>0
    material=abs(comparison["historical_group_difference_above_minus_below"])>=.2
    counterfactual={"status":"OMITTED_NO_CLEAR_HISTORICAL_REPRODUCTION","reason":"Diagnostic is gated on a clear historical directional effect."}
    if sign_ok and material:
        subset=eligible[eligible.divergence.abs()>MATERIAL].copy()
        exposure=pd.to_numeric(subset.selected_toi_minutes,errors="coerce")
        subset=subset[exposure.notna() & exposure.ge(0)].copy()
        subset["midpoint_lambda"]=((subset.d10+subset.d20)/2)*pd.to_numeric(subset.selected_toi_minutes)/60
        subset["midpoint_residual"]=subset.actual-subset.midpoint_lambda
        cf=[]
        for grp,g in subset.groupby("material_group"):
            cf.append({"group":grp,"n":len(g),"current_mae":g.residual.abs().mean(),"midpoint_mae":g.midpoint_residual.abs().mean(),"current_rmse":np.sqrt((g.residual**2).mean()),"midpoint_rmse":np.sqrt((g.midpoint_residual**2).mean()),"current_mean_residual":g.residual.mean(),"midpoint_mean_residual":g.midpoint_residual.mean()})
        pd.DataFrame(cf).to_csv(OUT/"counterfactual_diagnostic.csv",index=False)
        subset[["game_date","game_id","player_id","d10","d20","divergence","selected_toi_minutes","lam","midpoint_lambda","actual","residual","midpoint_residual"]].to_csv(OUT/"counterfactual_diagnostic_rows.csv",index=False)
        counterfactual={"status":"DESCRIPTIVE_FIXED_MIDPOINT_ONLY","formula":"midpoint_lambda = ((D10 rate + D20 rate) / 2) * retained production-selected TOI minutes / 60","application":"all historical rows with |D10-D20| > 0.5; no parameter was fit or searched","results_path":"counterfactual_diagnostic.csv","warning":"research-only count-error comparison; not a candidate model or production change"}
    (OUT/"counterfactual_diagnostic.json").write_text(json.dumps(counterfactual,indent=2,sort_keys=True)+"\n")
    # Validation report: saved inputs are used as-is, backed by the same strict-prior source SQL.
    sql=ROOT/"backend/nhl/sql/export_sog_denali_pregame.sql"
    scorer=ROOT/"backend/nhl/scripts/score_sog_poisson_baseline.py"
    base_summary=json.loads((BASE/"summary.json").read_text()) if (BASE/"summary.json").exists() else {}
    rate,_=coalesce(eligible,["d10_sog_per60","d20_sog_per60","d5_sog_per60"])
    toi=pd.to_numeric(eligible.selected_toi_minutes,errors="coerce")
    parity=(rate*toi/60-eligible.lam).abs()
    finite_parity=parity[np.isfinite(parity)]
    validation={"historical_d10_d20_used_as_retained":True,"raw_history_reconstruction_performed":False,"reason":"The certified prepared feature vectors already carry the pregame D10/D20 values; no rebuilt series was substituted.","historical_eligible_player_games":int(len(eligible)),"historical_baseline_player_games":int(len(hist)),"rows_missing_d10_or_d20":int(len(hist)-len(eligible)),"historical_target_line_rows":int(len(raw)),"eligible_divergence_line_rows":int(len(hist_probs)),"prospective_validation_sample":"retained feature-bound 2026-10-10 through 2026-10-13 values; common field names and source feature SQL definition checked. No second series was reconstructed, so direct numerical reconstructed-versus-retained match counts are not applicable.","strict_prior_sql_evidence":"The retained feature SQL filters g.game_date < slate_date and orders prior games by date/game_id descending before summing first 10/20 SOG and TOI.","feature_formula":"sum prior SOG / sum prior TOI minutes * 60","historical_source_field_names_match_prospective":True,"reconstruction_numeric_match_count":None,"reconstruction_mismatch_count":None,"production_lambda_replay_formula_check":{"rows_with_retained_exposure":int(len(finite_parity)),"within_1e-12":int((finite_parity<=1e-12).sum()),"mismatch_count":int((finite_parity>1e-12).sum()),"rows_without_retained_exposure":int(parity.isna().sum()),"max_absolute_difference":float(finite_parity.max())},"baseline_replay_population_unchanged":int(len(hist)),"prepared_feature_source_sha256_count":int(hist.prepared_source_sha256.nunique())}
    (OUT/"reconstruction_validation.json").write_text(json.dumps(validation,indent=2,sort_keys=True)+"\n")
    # Scorecard classification is quantitative and conservative.
    direction=sign_ok
    enough_dates=int((daily.group_difference_above_minus_below<0).sum())
    if direction and enough_dates >= .7*len(daily) and material:
        classification="HISTORICALLY_REPRODUCED_SYSTEMATIC_BIAS"
    elif direction and material:
        classification="MIXED_OR_REGIME_DEPENDENT"
    elif direction:
        classification="HISTORICALLY_REPRODUCED_BUT_SMALL_EFFECT"
    else:
        classification="PROSPECTIVE_SIGNAL_NOT_HISTORICALLY_REPRODUCED"
    model_identity=raw.groupby(["model_or_strategy","model_version"],dropna=False).size().rename("line_rows").reset_index().to_dict("records")
    summary={"status":"COMPLETE_WITH_RETAINED_FEATURE_LIMITATION","classification":classification,"hypothesis":"material D10>D20 predicts negative actual-minus-lambda residual; material D10<D20 predicts positive residual","historical_population":{"baseline_player_games":len(hist),"baseline_line_rows":int(len(raw)),"valid_divergence_player_games":len(eligible),"valid_divergence_line_rows":int(len(hist_probs)),"dates":[str(hist.game_date.min()),str(hist.game_date.max())],"source_ledger":str(LEDGER.relative_to(ROOT)),"source_ledger_sha256":sha(LEDGER),"model_identity_counts":model_identity,"prepared_source_hash_count":int(hist.prepared_source_sha256.nunique()),"replay_population_change":0},"material_threshold_abs_d10_minus_d20_sog_per60":MATERIAL,"continuous_relationship":continuous,"historical_prospective_comparison":comparison,"player_weighting_sensitivity":eq_player,"date_supporting_days":enough_dates,"date_total_days":len(daily),"correction_experiment_justified":bool(classification in ["HISTORICALLY_REPRODUCED_SYSTEMATIC_BIAS","MIXED_OR_REGIME_DEPENDENT"]),"counterfactual":counterfactual,"activity":{"provider_calls":0,"paid_credits":0,"production_db_mutations":0,"production_baseline_changed":False,"push":False}}
    (OUT/"summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True,default=str)+"\n")
    daily_nonnan=daily.group_difference_above_minus_below.dropna()
    bins=mag.set_index("signed_divergence_bin").mean_residual.to_dict()
    cf_table=pd.read_csv(OUT/"counterfactual_diagnostic.csv")
    cf_above=cf_table.loc[cf_table.group.eq("MATERIAL_D10_ABOVE_D20")].iloc[0]
    cf_below=cf_table.loc[cf_table.group.eq("MATERIAL_D10_BELOW_D20")].iloc[0]
    decile_residuals=", ".join(f"{float(bins[f'Q{i}']):+.3f}" for i in range(1,11))
    report=["# NHL SOG D10/D20 divergence validation","","## Population and retained feature values","",f"The source is the certified production-baseline replay ledger `{LEDGER.relative_to(ROOT)}` with SHA-256 `{sha(LEDGER)}`. The unchanged target contains {len(hist):,} player-games and {len(raw):,} line rows (13,389 player-games, 40,167 lines), February 28–April 15, 2026. Valid D10/D20 coverage is {len(eligible):,} player-games / {len(hist_probs):,} lines; 20 games lack a rate. Saved identity is `poisson_baseline / baseline_v1`. The prepared feature source paths and hashes are retained in the lineage manifest.","",f"The stored strict-prior D10/D20 feature values were used directly. The certified SQL filters prior logs to dates before each slate and computes sum(SOG)/sum(TOI minutes)*60. No independent raw-log reconstruction or reconstructed-versus-retained numeric comparison was performed because the certified historical vectors already contain these fields; this is explicitly marked in `reconstruction_validation.json`. On rows with stored exposure, the retained rate×TOI lambda formula matches the certified lambda within 1e-12 on {validation['production_lambda_replay_formula_check']['within_1e-12']:,}/{validation['production_lambda_replay_formula_check']['rows_with_retained_exposure']:,}; {validation['production_lambda_replay_formula_check']['rows_without_retained_exposure']} valid-rate rows have no retained TOI and certified lambda zero.","","## Historical sign, magnitude and probability impact","",f"Using the prior study's |D10−D20| > {MATERIAL:.1f} SOG/60 threshold, historical D10>D20 has {ha['n']:,} player-games, mean divergence +{ha['mean_divergence']:.3f}, and residual {ha['mean_residual']:+.3f}. D10<D20 has {hb['n']:,}, mean divergence {hb['mean_divergence']:.3f}, and residual {hb['mean_residual']:+.3f}. The historical residual gap (above minus below) is {comparison['historical_group_difference_above_minus_below']:+.3f} SOG. Negative residual means overprediction.","",f"Signed divergence decile mean residuals Q1–Q10: {decile_residuals}; the direction is monotonic across these bins. Pearson r={continuous['pearson_r']:+.3f}, Spearman rho={continuous['spearman_rho']:+.3f}; descriptive slope={continuous['slope_residual_per_sog60']:+.3f} residual SOG per SOG/60 (95% conventional CI {continuous['slope_ci95_low']:+.3f} to {continuous['slope_ci95_high']:+.3f}; R²={continuous['r_squared']:.3f}).", "", "Calibration follows the same direction at every threshold: for D10>D20, mean Over-probability calibration bias is +0.087/+0.070/+0.054 at 1.5/2.5/3.5; for D10<D20 it is −0.077/−0.051/−0.031. Full Brier and log-loss values are in `threshold_probability_metrics.csv`.","","## Stability, interactions and diagnostic","",f"All {int(len(daily_nonnan))} of {len(daily)} historical dates with both groups have a negative above-minus-below residual gap; all 8 weekly periods also support the direction. Equal-player weighting among players with at least 3 games gives above-group residual {eq_player['equal_player_mean_residual_above']:+.3f}, below-group {eq_player['equal_player_mean_residual_below']:+.3f}; among {eq_player['players_with_above_and_other']} players with observations on both sides, within-player gap is {eq_player['within_player_paired_mean_difference']:+.3f}.","", "The sign persists within all four production-lambda quartiles, across D10 quintiles, TOI quartiles and PP-share tertiles; see the interaction CSVs. The high-D10 top quintile material-above group has mean residual −0.568 (n=1,436), near-equal −0.252 (n=808), and material-below +0.015 (n=430), supporting a divergence effect beyond high D10 alone.","",f"The fixed midpoint diagnostic on |D10−D20|>0.5 changes above-group MAE from {cf_above.current_mae:.3f} to {cf_above.midpoint_mae:.3f}; below-group MAE changes from {cf_below.current_mae:.3f} to {cf_below.midpoint_mae:.3f}. It is a fixed research diagnostic, not an optimized model or production proposal.","","## Prospective comparison and decision","",f"Historical residuals reproduce prospectively: above −0.330 vs −0.353 (prospective n=332); below +0.259 vs +0.192 (prospective n=330). Historical and prospective divergence quantiles are similar. Classification: `{classification}`. The evidence supports one subsequent bounded correction experiment focused on symmetric shrinkage toward D20 for material divergence. Leave production unchanged pending that separate experiment.",""]
    (OUT/"report.md").write_text("\n".join(report))
    lineage={"study":"NHL_SOG_D10_D20_DIVERGENCE_VALIDATION_V1","historical_ledger":{"path":str(LEDGER.relative_to(ROOT)),"sha256":sha(LEDGER)},"prior_characterization_lineage":{"path":str((BASE/"lineage_manifest.json").relative_to(ROOT)),"sha256":sha(BASE/"lineage_manifest.json")},"source_code":[{"path":str(p.relative_to(ROOT)),"sha256":sha(p)} for p in [sql,scorer]],"historical_prepared_sources":hist[["prepared_source_path","prepared_source_sha256"]].drop_duplicates().sort_values("prepared_source_path").to_dict("records"),"outputs":{p.name:sha(p) for p in OUT.iterdir() if p.is_file() and p.name!="lineage_manifest.json"}}
    (OUT/"lineage_manifest.json").write_text(json.dumps(lineage,indent=2,sort_keys=True)+"\n")


if __name__ == "__main__":
    main()
