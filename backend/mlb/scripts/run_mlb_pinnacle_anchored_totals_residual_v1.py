#!/usr/bin/env python3
"""Bounded research-only Pinnacle-anchored MLB totals residual experiment."""
from __future__ import annotations

import glob, hashlib, json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance
from sklearn.linear_model import HuberRegressor, LogisticRegression, Ridge
from sklearn.metrics import brier_score_loss, log_loss, mean_absolute_error, mean_squared_error
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/"artifacts/analysis/model_development/mlb_pinnacle_anchored_totals_residual_v1/2026-08-10"
JOIN=ROOT/"artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/totals_pinnacle_join.csv"
POP=ROOT/"artifacts/analysis/model_development/mlb_totals_prediction_foundation_v1/2026-08-06/certified_totals_game_population.csv"
SEED=20260810
BASE_FEATURES=["pinnacle_total_line","league_total","home_rs","home_ra","away_rs","away_ra","home_wp","away_wp","home_last10_wp","home_last10_diff","away_last10_wp","away_last10_diff","home_rest","away_rest","home_games","away_games","month_sin","month_cos","starter_state_available","bullpen_state_available","park_state_available","weather_state_available","lineup_state_available","doubleheader_state_available"]

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def clip(p): return np.clip(np.asarray(p,float),1e-6,1-1e-6)
def reg_metrics(actual,line,corr):
    pred=line+corr; base=actual-line; err=actual-pred
    return {"games":len(actual),"pinnacle_mae":float(np.mean(abs(base))),"pinnacle_rmse":float(np.mean(base**2)**.5),"pinnacle_bias":float(np.mean(line-actual)),"corrected_mae":float(np.mean(abs(err))),"corrected_rmse":float(np.mean(err**2)**.5),"corrected_bias":float(np.mean(pred-actual)),"mae_improvement":float(np.mean(abs(base))-np.mean(abs(err))),"rmse_improvement":float(np.mean(base**2)**.5-np.mean(err**2)**.5),"absolute_bias_change":float(abs(np.mean(pred-actual))-abs(np.mean(line-actual))),"residual_correlation":float(np.corrcoef(corr,base)[0,1]) if np.std(corr)>0 else np.nan,"mean_predicted_correction":float(np.mean(corr)),"correction_sd":float(np.std(corr)),"positive_correction_pct":float(np.mean(corr>0)),"negative_correction_pct":float(np.mean(corr<0)),"mean_absolute_correction":float(np.mean(abs(corr))),"median_absolute_correction":float(np.median(abs(corr)))}
def bin_metrics(y,p):
    p=clip(p); y=np.asarray(y,int)
    return {"games":len(y),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,p,labels=[0,1])),"calibration_error":float(np.mean(p)-np.mean(y)),"predicted_over_rate":float(np.mean(p)),"observed_over_rate":float(np.mean(y))}
def residual_probability(corr,train_errors): return np.array([np.mean(c+train_errors>0) for c in np.asarray(corr)])

def population():
    j=pd.read_csv(JOIN); p=pd.read_csv(POP)
    keep=["game_pk","scheduled_start_utc","home_team_abbr","away_team_abbr"]+[c for c in BASE_FEATURES if c!="pinnacle_total_line"]
    d=j.merge(p[keep],on="game_pk",how="inner",validate="one_to_one")
    d["scheduled_start_utc"]=pd.to_datetime(d.scheduled_start_utc,utc=True); d["provider_snapshot_utc"]=pd.to_datetime(d.provider_snapshot_utc,utc=True)
    d["snapshot_lead_minutes"]=(d.scheduled_start_utc-d.provider_snapshot_utc).dt.total_seconds()/60
    d["market_total_residual"]=d.final_total-d.pinnacle_total_line; d["regulation_nine_residual"]=d.regulation_total-d.pinnacle_total_line
    d["is_push"]=d.final_total.eq(d.pinnacle_total_line); d["actual_over"]=np.where(d.is_push,np.nan,(d.final_total>d.pinnacle_total_line).astype(float))
    # Fail closed under the accepted exact-binding and pregame contracts.
    d=d[d.mapping_status.str.startswith("EXACT") & d.snapshot_lead_minutes.gt(0) & d.pinnacle_over_price.notna() & d.pinnacle_under_price.notna() & d.final_total.notna()].sort_values(["scheduled_start_utc","game_pk"]).reset_index(drop=True)
    dates=sorted(d.game_date.unique()); dev_end=dates[int(len(dates)*.60)-1]; val_end=dates[int(len(dates)*.80)-1]
    d["temporal_split"]=np.select([d.game_date<=dev_end,d.game_date<=val_end],["DEVELOPMENT","VALIDATION"],default="FINAL_HOLDOUT")
    cols=["game_pk","game_date","scheduled_start_utc","home_team_abbr","away_team_abbr","pinnacle_total_line","pinnacle_over_price","pinnacle_under_price","pinnacle_over_no_vig_probability","provider_snapshot_utc","snapshot_lead_minutes","mapping_status","final_total","regulation_total","market_total_residual","regulation_nine_residual","actual_over","is_push","temporal_split","source_sha256"]
    d[cols].to_csv(OUT/"residual_population_manifest.csv",index=False)
    contract={"experiment":"MLB_PINNACLE_ANCHORED_TOTALS_RESIDUAL_V1","ordering":"scheduled_start_utc, game_pk; whole game dates","development":{"through":dev_end,"games":int((d.temporal_split=='DEVELOPMENT').sum())},"validation":{"from":d.loc[d.temporal_split.eq('VALIDATION'),'game_date'].min(),"through":val_end,"games":int((d.temporal_split=='VALIDATION').sum())},"final_holdout":{"from":d.loc[d.temporal_split.eq('FINAL_HOLDOUT'),'game_date'].min(),"through":d.game_date.max(),"games":int((d.temporal_split=='FINAL_HOLDOUT').sum())},"selection":"validation MAE only; final holdout untouched","snapshot_contract":"accepted latest Pinnacle observation at/before frozen pregame lead policy"}
    (OUT/"temporal_split_contract.json").write_text(json.dumps(contract,indent=2)+"\n")
    concepts={"pinnacle_total_line":"market anchor","home_rs|away_rs|home_last10_diff|away_last10_diff":"team offense / recent production","home_ra|away_ra":"team run prevention","home_wp|away_wp|home_last10_wp|away_last10_wp":"strict-prior differential state","home_games|away_games":"history depth","starter_state_available":"starter state availability/fallback only; granular starter fields unavailable","bullpen_state_available":"bullpen state availability/fallback only; granular workload unavailable","park_state_available":"park-state availability only; granular park/elevation unavailable","weather_state_available":"environment availability only","home_rest|away_rest|doubleheader_state_available":"schedule/rest context","league_total|month_sin|month_cos":"season run environment"}
    rows=[]
    for f in BASE_FEATURES:
        note=next((v for k,v in concepts.items() if f in k.split('|')),"certified strict-prior numeric state")
        rows.append({"feature":f,"primary_predictor":True,"strict_prior_status":"ACCEPTED_PRE_GAME_STATE","missing_rate":d[f].isna().mean(),"concept":note,"explicitly_excluded":False})
    for f in ["expected_total","model_over_probability","moneyline_output","run_line_output","multi_book_consensus","bookmaker","later_pinnacle_observation"]: rows.append({"feature":f,"primary_predictor":False,"strict_prior_status":"EXCLUDED_PRIMARY","missing_rate":np.nan,"concept":"required exclusion","explicitly_excluded":True})
    pd.DataFrame(rows).to_csv(OUT/"residual_feature_manifest.csv",index=False)
    return d

def factories():
    return {"MODEL_A_REGULARIZED_LINEAR":lambda:make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),Ridge(alpha=10.0)),"MODEL_B_ROBUST_LINEAR":lambda:make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),HuberRegressor(epsilon=1.35,alpha=.01,max_iter=1000)),"MODEL_C_SHALLOW_HGB":lambda:HistGradientBoostingRegressor(max_iter=100,max_leaf_nodes=15,min_samples_leaf=25,learning_rate=.05,l2_regularization=1.0,early_stopping=False,random_state=SEED)}

def primary_models(d):
    X=d[BASE_FEATURES]; y=d.market_total_residual; dev=d.temporal_split.eq("DEVELOPMENT"); val=d.temporal_split.eq("VALIDATION"); hold=d.temporal_split.eq("FINAL_HOLDOUT")
    rows=[]; validation_preds={}
    for name,fac in factories().items():
        q=fac().fit(X[dev],y[dev]); pv=q.predict(X[val]); validation_preds[name]=pv; rows.append({"phase":"VALIDATION","model":name,**reg_metrics(d.loc[val,"final_total"].to_numpy(),d.loc[val,"pinnacle_total_line"].to_numpy(),pv)})
    cmp=pd.DataFrame(rows); selected=cmp.sort_values(["corrected_mae","corrected_rmse"]).iloc[0].model
    # Freeze selection, then refit each fixed candidate on development+validation once.
    train=dev|val; hold_preds={}; models={}
    for name,fac in factories().items():
        q=fac().fit(X[train],y[train]); ph=q.predict(X[hold]); hold_preds[name]=ph; models[name]=q; rows.append({"phase":"FINAL_HOLDOUT","model":name,**reg_metrics(d.loc[hold,"final_total"].to_numpy(),d.loc[hold,"pinnacle_total_line"].to_numpy(),ph)})
    comp=pd.DataFrame(rows); comp["selected_on_validation"]=comp.model.eq(selected); comp.to_csv(OUT/"residual_model_comparison.csv",index=False)
    h=comp[(comp.phase.eq("FINAL_HOLDOUT"))&comp.model.eq(selected)].copy()
    ac=np.abs(hold_preds[selected]); edges=[0,.25,.5,.75,1,np.inf]; names=["correction_pct_lt_0_25","correction_pct_0_25_0_49","correction_pct_0_50_0_74","correction_pct_0_75_0_99","correction_pct_ge_1_00"]
    for name,lo,hi in zip(names,edges[:-1],edges[1:]): h[name]=float(np.mean((ac>=lo)&(ac<hi)))
    h.to_csv(OUT/"residual_holdout_metrics.csv",index=False)
    pred=d[["game_pk","game_date","temporal_split","final_total","regulation_total","pinnacle_total_line","pinnacle_over_no_vig_probability","actual_over","is_push"]+BASE_FEATURES[1:]].copy(); pred["predicted_residual"]=np.nan
    pred.loc[val,"predicted_residual"] = validation_preds[selected]; pred.loc[hold,"predicted_residual"] = hold_preds[selected]
    # Comparable in-sample development predictions only for descriptive split stability.
    qdev=factories()[selected]().fit(X[dev],y[dev]); pred.loc[dev,"predicted_residual"]=qdev.predict(X[dev])
    pred["corrected_total"]=pred.pinnacle_total_line+pred.predicted_residual
    pred.to_csv(OUT/"selected_residual_predictions.csv",index=False)
    return selected,qdev,pred,dev,val,hold

def binary(d,dev,val,hold):
    features=["pinnacle_over_no_vig_probability"]+BASE_FEATURES
    rows=[]; probs={}
    for phase,train,test in [("VALIDATION",dev,val),("FINAL_HOLDOUT",dev|val,hold)]:
        tr=train & ~d.is_push; te=test & ~d.is_push
        q=make_pipeline(SimpleImputer(strategy="median"),StandardScaler(),LogisticRegression(C=.25,max_iter=2000,random_state=SEED)).fit(d.loc[tr,features],d.loc[tr,"actual_over"])
        p=q.predict_proba(d.loc[te,features])[:,1]; probs[phase]=(d.index[te],p)
        for name,pp in [("PINNACLE_NO_VIG",d.loc[te,"pinnacle_over_no_vig_probability"]),("MODEL_D_DIRECT_OVER_UNDER_LOGISTIC",p)]: rows.append({"phase":phase,"model":name,"pushes_excluded":int((test&d.is_push).sum()),**bin_metrics(d.loc[te,"actual_over"],pp)})
    out=pd.DataFrame(rows); base=out[out.model.eq("PINNACLE_NO_VIG")].set_index("phase"); mod=out[out.model.str.startswith("MODEL_D")].set_index("phase")
    out["brier_difference_model_minus_pinnacle"]=out.phase.map((mod.brier-base.brier).to_dict()); out["log_loss_difference_model_minus_pinnacle"]=out.phase.map((mod.log_loss-base.log_loss).to_dict());out["abs_calibration_difference_model_minus_pinnacle"]=out.phase.map((mod.calibration_error.abs()-base.calibration_error.abs()).to_dict())
    out.to_csv(OUT/"binary_market_probability_comparison.csv",index=False)
    return probs

def analyses(d,pred,selected,dev,val,hold,binary_probs,model):
    train=dev|val; train_err=d.loc[train,"market_total_residual"].to_numpy()-pred.loc[train,"predicted_residual"].to_numpy()
    pred["residual_over_probability"]=residual_probability(pred.predicted_residual,train_err)
    pred["correction_band"]=pd.cut(abs(pred.predicted_residual),[-1,.25,.5,.75,1,np.inf],labels=["<0.25","0.25-0.49","0.50-0.74","0.75-0.99",">=1.00"],right=False)
    bands=[]
    for phase in ["VALIDATION","FINAL_HOLDOUT"]:
      for band,g in pred[pred.temporal_split.eq(phase)].groupby("correction_band",observed=True):
        ng=g[~g.is_push]; bands.append({"phase":phase,"band":band,"games":len(g),"mean_predicted_correction":g.predicted_residual.mean(),"actual_mean_residual":(g.final_total-g.pinnacle_total_line).mean(),"pinnacle_mae":abs(g.final_total-g.pinnacle_total_line).mean(),"corrected_model_mae":abs(g.final_total-g.corrected_total).mean(),"pinnacle_brier":brier_score_loss(ng.actual_over,ng.pinnacle_over_no_vig_probability) if len(ng) else np.nan,"residual_model_brier":brier_score_loss(ng.actual_over,ng.residual_over_probability) if len(ng) else np.nan,"direction_accuracy":np.mean(np.sign(g.predicted_residual)==np.sign(g.final_total-g.pinnacle_total_line))})
    pd.DataFrame(bands).to_csv(OUT/"residual_correction_bands.csv",index=False)
    directions=[]
    for phase in ["VALIDATION","FINAL_HOLDOUT"]:
      for direction,g in pred[pred.temporal_split.eq(phase)].groupby(np.where(pred[pred.temporal_split.eq(phase)].predicted_residual>=0,"PINNACLE_TOO_LOW","PINNACLE_TOO_HIGH")):
        directions.append({"phase":phase,"direction":direction,"games":len(g),**reg_metrics(g.final_total.to_numpy(),g.pinnacle_total_line.to_numpy(),g.predicted_residual.to_numpy())})
    pd.DataFrame(directions).to_csv(OUT/"residual_direction_analysis.csv",index=False)
    # Temporal stability at split, month and sequential blocks. Binary probability is empirical residual distribution for uniform coverage.
    rows=[]; pred["month"]=pred.game_date.str[:7]; pred["rolling_50_block"]=(np.arange(len(pred))//50).astype(int)
    groups=[("split",pred.groupby("temporal_split")),("month",pred.groupby("month")),("rolling_50",pred.groupby("rolling_50_block"))]
    for typ,grp in groups:
      for key,g in grp:
        ng=g[~g.is_push]; r={"slice_type":typ,"slice_value":key,**reg_metrics(g.final_total.to_numpy(),g.pinnacle_total_line.to_numpy(),g.predicted_residual.to_numpy()),"average_absolute_correction":abs(g.predicted_residual).mean()}
        r["binary_brier_difference_vs_pinnacle"]=(brier_score_loss(ng.actual_over,ng.residual_over_probability)-brier_score_loss(ng.actual_over,ng.pinnacle_over_no_vig_probability)) if len(ng) else np.nan; rows.append(r)
    pd.DataFrame(rows).to_csv(OUT/"residual_temporal_stability.csv",index=False)
    # Fixed attribution slices: unavailable granular concepts are retained as explicit NA limitations.
    attrs=[]
    holdp=pred[pred.temporal_split.eq("FINAL_HOLDOUT")].copy()
    slice_specs={"pinnacle_total_band":pd.cut(holdp.pinnacle_total_line,[-np.inf,8,9,10,np.inf],labels=["LOW_<8","8_TO_<9","9_TO_<10","HIGH_>=10"]),"starter_history_depth":pd.cut(holdp[["home_games","away_games"]].min(axis=1),[-1,25,50,100,np.inf],labels=["SPARSE","LOW","MEDIUM","DEEP"]),"starter_state_available":holdp.starter_state_available,"bullpen_state_available":holdp.bullpen_state_available,"park_state_available":holdp.park_state_available,"doubleheader_state_available":holdp.doubleheader_state_available,"recent_run_environment":pd.cut(holdp[["home_rs","away_rs"]].mean(axis=1),[-np.inf,4,4.5,5,np.inf],labels=["LOW","MID_LOW","MID_HIGH","HIGH"]),"month":holdp.game_date.str[:7]}
    for concept,values in slice_specs.items():
      holdp["slice_value"]=values
      for value,g in holdp.groupby("slice_value",observed=True): attrs.append({"concept":concept,"slice_value":value,"games":len(g),"systematic_pinnacle_residual":(g.final_total-g.pinnacle_total_line).mean(),"mean_model_correction":g.predicted_residual.mean(),"pinnacle_mae":abs(g.final_total-g.pinnacle_total_line).mean(),"corrected_mae":abs(g.final_total-g.corrected_total).mean(),"mae_improvement":abs(g.final_total-g.pinnacle_total_line).mean()-abs(g.final_total-g.corrected_total).mean(),"direction_aligned":np.sign((g.final_total-g.pinnacle_total_line).mean())==np.sign(g.predicted_residual.mean())})
    for unavailable in ["strong_weak_starter_quality","bullpen_stress_workload","park_identity","elevation","home_away_indicator","day_night","starter_handedness"]: attrs.append({"concept":unavailable,"slice_value":"UNAVAILABLE_AS_CERTIFIED_VALUE_AT_THIS_GRAIN","games":0,"systematic_pinnacle_residual":np.nan,"mean_model_correction":np.nan,"pinnacle_mae":np.nan,"corrected_mae":np.nan,"mae_improvement":np.nan,"direction_aligned":False})
    pd.DataFrame(attrs).to_csv(OUT/"residual_attribution_slices.csv",index=False)
    # Validation-only permutation importance; no final-holdout tuning.
    pi=permutation_importance(model,d.loc[val,BASE_FEATURES],d.loc[val,"market_total_residual"],scoring="neg_mean_absolute_error",n_repeats=10,random_state=SEED)
    novelty=[]
    for f,imp,sd in sorted(zip(BASE_FEATURES,pi.importances_mean,pi.importances_std),key=lambda x:-x[1]):
        cls="plausible incremental baseball information" if imp>sd and imp>0.005 and f!="pinnacle_total_line" else "likely market-redundant" if f=="pinnacle_total_line" or abs(imp)<=0.005 else "unstable/noisy"
        novelty.append({"feature":f,"validation_mae_permutation_importance":imp,"importance_sd":sd,"classification":cls,"causal_claim":False})
    pd.DataFrame(novelty).to_csv(OUT/"residual_feature_novelty.csv",index=False)

def v1_diag(d,selected,dev,val,hold):
    # Optional, after primary selection is frozen.
    diag_features=BASE_FEATURES+["expected_total","model_over_probability"]
    rows=[]
    for phase,train,test in [("VALIDATION",dev,val),("FINAL_HOLDOUT",dev|val,hold)]:
      for label,features in [("A_PRIMARY_RAW_STATE_RESIDUAL",BASE_FEATURES),("B_PRIMARY_PLUS_FROZEN_V1",diag_features)]:
        q=factories()[selected]().fit(d.loc[train,features],d.loc[train,"market_total_residual"]); corr=q.predict(d.loc[test,features]); rows.append({"phase":phase,"diagnostic":label,**reg_metrics(d.loc[test,"final_total"].to_numpy(),d.loc[test,"pinnacle_total_line"].to_numpy(),corr)})
    o=pd.DataFrame(rows); a=o[o.diagnostic.str.startswith("A_")].set_index("phase");b=o[o.diagnostic.str.startswith("B_")].set_index("phase");o["v1_mae_delta_B_minus_A"]=o.phase.map((b.corrected_mae-a.corrected_mae).to_dict());o["v1_rmse_delta_B_minus_A"]=o.phase.map((b.corrected_rmse-a.corrected_rmse).to_dict());o.to_csv(OUT/"v1_incremental_diagnostic.csv",index=False)

def prospective_consistency():
    summaries=[]; grades=[]; market=[]
    for day in ["06","07","08","09"]:
        base=ROOT/f"artifacts/analysis/model_development/mlb_totals_prospective_shadow_v1/2026-08-{day}"
        sf=list(base.glob("*totals_grade_summary.json")); gf=list(base.glob("*totals_grading.csv")); mf=list(base.glob("*multibook_market_results.csv"))
        if sf:summaries.append(json.loads(sf[0].read_text()))
        if gf: grades.append(pd.read_csv(gf[0]))
        if mf: market.append(pd.read_csv(mf[0]))
    g=pd.concat(grades,ignore_index=True); m=pd.concat(market,ignore_index=True) if market else pd.DataFrame()
    # Pinnacle comparable rows live in the immutable existing-market attachment
    # inventory rather than the primary consensus result table. Use the latest
    # certified observation at or before each frozen prediction.
    attachments=[]
    for day in ["06","07","08","09"]:
        paths=list((ROOT/f"artifacts/analysis/model_development/mlb_totals_prospective_shadow_v1/2026-08-{day}").glob("*existing_market_attachments.csv"))
        if paths: attachments.append(pd.read_csv(paths[0]))
    if attachments:
        a=pd.concat(attachments,ignore_index=True); a=a[a.bookmaker_key.astype(str).str.lower().eq("pinnacle") & a.timing_relationship.eq("AT_OR_BEFORE_PREDICTION") & a.timing_status.eq("PREGAME_CERTIFIED")].copy();a["market_timestamp_utc"]=pd.to_datetime(a.market_timestamp_utc,utc=True);a=a.sort_values("market_timestamp_utc").drop_duplicates("game_pk",keep="last")
        pinnacle=a.merge(g[["game_pk","official_final_total"]],on="game_pk",how="inner"); pinnacle["sportsbook_line_absolute_error"]=abs(pinnacle.official_final_total-pinnacle.total_line)
    else: pinnacle=pd.DataFrame()
    n=sum(x["official_finals"] for x in summaries); model_mae=np.average([x["model_mae_final"] for x in summaries],weights=[x["official_finals"] for x in summaries]);bias=np.average([x["model_signed_bias_final"] for x in summaries],weights=[x["official_finals"] for x in summaries]);crps=np.average([x["model_crps_final"] for x in summaries],weights=[x["official_finals"] for x in summaries]);cons=np.average([x["consensus_market_mae"] for x in summaries],weights=[x["official_finals"] for x in summaries])
    pmae=float(pinnacle.sportsbook_line_absolute_error.mean()) if len(pinnacle) else np.nan
    text=f"""# Historical versus prospective residual consistency

The latest certified completed prospective slate is 2026-08-09. Across August 6-9 there are {n} graded games: frozen totals model MAE {model_mae:.6f}, signed bias {bias:.6f}, and CRPS {crps:.6f}. Consensus MAE is {cons:.6f}. Pinnacle is comparable on {len(pinnacle)} deduplicated games with MAE {pmae:.6f}.

These rows were read only and were not admitted to training. Directional compatibility is assessed in the concise report against the frozen historical holdout; the short live window cannot establish generalization by itself. Market observations in this prospective ledger are labeled post-prediction, so this comparison is descriptive rather than a synchronized replacement for the historical snapshot contract.
"""
    (OUT/"historical_vs_prospective_residual_consistency.md").write_text(text)
    return {"graded":n,"model_mae":model_mae,"bias":bias,"crps":crps,"pinnacle_games":len(pinnacle),"pinnacle_mae":pmae,"consensus_mae":cons}

def report(d,selected,pred,pros):
    h=pd.read_csv(OUT/"residual_holdout_metrics.csv").iloc[0]; b=pd.read_csv(OUT/"binary_market_probability_comparison.csv"); bp=b[(b.phase.eq("FINAL_HOLDOUT"))&b.model.eq("PINNACLE_NO_VIG")].iloc[0]; bm=b[(b.phase.eq("FINAL_HOLDOUT"))&b.model.str.startswith("MODEL_D")].iloc[0]
    dirs=pd.read_csv(OUT/"residual_direction_analysis.csv"); stable=pd.read_csv(OUT/"residual_temporal_stability.csv"); months=stable[stable.slice_type.eq("month")]
    improves_val=pd.read_csv(OUT/"residual_model_comparison.csv").query("phase=='VALIDATION' and selected_on_validation").iloc[0].mae_improvement>0
    improves_hold=h.mae_improvement>0; distributed=(months.mae_improvement>0).mean()>=.6
    if improves_val and improves_hold and distributed and h.mae_improvement>=.10 and bm.brier<=bp.brier and bm.log_loss<=bp.log_loss: decision="TOTALS_MARKET_RESIDUAL_INCREMENTAL_INFORMATION_PRESENT"
    elif improves_val and improves_hold and h.mae_improvement>0 and not distributed: decision="TOTALS_MARKET_RESIDUAL_RESULT_MIXED"
    elif improves_val and improves_hold and h.mae_improvement<.10: decision="TOTALS_MARKET_RESIDUAL_STATISTICALLY_POSITIVE_BUT_IMMATERIAL"
    elif improves_val != improves_hold or (dirs.mae_improvement>0).any(): decision="TOTALS_MARKET_RESIDUAL_RESULT_MIXED"
    else: decision="TOTALS_MARKET_RESIDUAL_NO_INCREMENTAL_SIGNAL"
    practical=decision=="TOTALS_MARKET_RESIDUAL_INCREMENTAL_INFORMATION_PRESENT"
    novelty=pd.read_csv(OUT/"residual_feature_novelty.csv").head(5); v1=pd.read_csv(OUT/"v1_incremental_diagnostic.csv"); v1delta=v1.query("phase=='FINAL_HOLDOUT' and diagnostic=='B_PRIMARY_PLUS_FROZEN_V1'").iloc[0].v1_mae_delta_B_minus_A
    (OUT/"current_slate_shadow_status.md").write_text(f"# Current-slate shadow status\n\nHistorical practical bar cleared: **{practical}**. {'A no-write August 10 shadow was not emitted because the practical bar did not clear.' if not practical else 'The practical bar cleared, but no synchronized already-acquired August 10 Pinnacle snapshot and strict-prior feature row set was available without new acquisition; no shadow was emitted.'} No prospective ledger was mutated.\n")
    text=f"""# MLB Pinnacle-Anchored Totals Residual v1

Experiment: `MLB_PINNACLE_ANCHORED_TOTALS_RESIDUAL_V1`

## Declaration

`{decision}`

## Frozen result

- Exact population: {len(d)} games, {d.game_date.min()} through {d.game_date.max()}; split counts {d.temporal_split.value_counts().to_dict()}.
- Selected on validation only: `{selected}`.
- Pinnacle holdout MAE/bias: {h.pinnacle_mae:.6f}/{h.pinnacle_bias:.6f}. Corrected holdout MAE/bias: {h.corrected_mae:.6f}/{h.corrected_bias:.6f}; MAE improvement {h.mae_improvement:.6f} runs.
- Holdout predicted-vs-actual residual correlation: {h.residual_correlation:.6f}; typical correction mean absolute {h.mean_absolute_correction:.6f}, median absolute {h.median_absolute_correction:.6f} runs.
- Pinnacle holdout Brier/log loss: {bp.brier:.6f}/{bp.log_loss:.6f}. Direct residual classifier: {bm.brier:.6f}/{bm.log_loss:.6f}.
- Direction and fixed correction-band results are reported separately. Month-level improvement share: {(months.mae_improvement>0).mean():.1%}; the result is not treated as distributed when this is weak.
- Leading validation-only residual features: {', '.join(novelty.feature.astype(str))}. Importance is predictive, not causal.
- Frozen V1 diagnostic holdout MAE delta when added after the primary freeze: {v1delta:.6f} runs (negative favors adding V1); it did not alter primary selection.
- Prospective through August 9: {pros['graded']} graded games, model MAE/bias/CRPS {pros['model_mae']:.6f}/{pros['bias']:.6f}/{pros['crps']:.6f}; Pinnacle {pros['pinnacle_games']} comparable games MAE {pros['pinnacle_mae']:.6f}; consensus MAE {pros['consensus_mae']:.6f}.

## Boundaries

The accepted feature spine exposes team production/prevention, rest, run environment, and availability/fallback indicators, but not granular certified starter quality, bullpen workload, park/elevation, handedness, or day/night values at this snapshot grain. Their residual contribution remains unproven. No external data or additional market history was acquired. No deployment, public change, EV/staking calculation, wager rule, or prospective-ledger mutation occurred.
"""
    (OUT/"concise_mlb_pinnacle_anchored_totals_residual_v1.md").write_text(text)
    pd.DataFrame([{"decision":decision,"practical_bar_cleared":practical,"selected_model":selected,"population":len(d),"holdout_games":int((d.temporal_split=='FINAL_HOLDOUT').sum())}]).to_csv(OUT/"residual_decision.csv",index=False)
    files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!="reproducibility_hashes.sha256"); (OUT/"reproducibility_hashes.sha256").write_text("".join(f"{sha(x)}  {x.name}\n" for x in files))

def main():
    OUT.mkdir(parents=True,exist_ok=True); d=population(); selected,model,pred,dev,val,hold=primary_models(d); probs=binary(d,dev,val,hold); analyses(d,pred,selected,dev,val,hold,probs,model); v1_diag(d,selected,dev,val,hold); pros=prospective_consistency(); report(d,selected,pred,pros)

if __name__=="__main__": main()
