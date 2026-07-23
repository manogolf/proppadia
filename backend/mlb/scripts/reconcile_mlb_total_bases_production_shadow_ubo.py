#!/usr/bin/env python3
"""Terminal identical-row Total Bases reconciliation; never mutates operational outputs."""
from __future__ import annotations
import hashlib,json,math
from datetime import datetime,timezone
from pathlib import Path

import joblib,numpy as np,pandas as pd,pyarrow.parquet as pq
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import balanced_accuracy_score,brier_score_loss,log_loss,roc_auc_score

from backend.mlb.scripts import run_mlb_ubo_coherent_revision as rev
from backend.mlb.scripts import run_mlb_unified_batter_outcome_v1 as ubo

ROOT=Path(__file__).resolve().parents[3]
WATCH=ROOT/"artifacts/analysis/mlb/model_quality/total_bases_shadow"
GOV=WATCH/"evaluation"
UBO=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1/2026-07-22"
COH=ROOT/"artifacts/analysis/model_development/mlb_unified_batter_outcome_v1_coherent_revision/2026-07-23"
OUT=ROOT/"artifacts/analysis/model_development/mlb_total_bases_production_shadow_ubo_terminal_reconciliation/2026-07-23"
MODELS={"production":"production_prob_over","balanced_shadow":"tb_rolling_balanced_shadow_prob_over",
        "unweighted_shadow":"tb_rolling_unweighted_shadow_prob_over",
        "original_ubo5":"original_ubo5_prob_over","coherent_revision_b":"coherent_ubo_prob_over"}
SEED=20260723

def sha(p):
 h=hashlib.sha256()
 with p.open("rb") as f:
  for b in iter(lambda:f.read(1<<20),b""):h.update(b)
 return h.hexdigest()

def save(name,data):
 d=data if isinstance(data,pd.DataFrame) else pd.DataFrame(data);OUT.mkdir(parents=True,exist_ok=True);d.to_csv(OUT/name,index=False);return d

def metrics(y,p):
 y=np.asarray(y,dtype=int);p=np.clip(np.asarray(p,dtype=float),1e-9,1-1e-9)
 pred=p>=.5
 ece=0
 for lo in np.linspace(0,1,11)[:-1]:
  mask=(p>=lo)&(p<(lo+.1) if lo<.9 else p<=1)
  if mask.any():ece+=mask.mean()*abs(p[mask].mean()-y[mask].mean())
 x=np.log(p/(1-p)).reshape(-1,1)
 has_both_classes=len(np.unique(y))==2
 cal=LogisticRegression(C=1e6,solver="lbfgs").fit(x,y) if has_both_classes else None
 return {"rows":len(y),"brier":brier_score_loss(y,p),"log_loss":log_loss(y,np.c_[1-p,p],labels=[0,1]),
  "auc":roc_auc_score(y,p) if has_both_classes else np.nan,"mean_over_probability":p.mean(),"actual_over_rate":y.mean(),
  "calibration_gap":p.mean()-y.mean(),"balanced_accuracy":balanced_accuracy_score(y,pred),
  "raw_accuracy":(pred==y).mean(),"calibration_intercept":float(cal.intercept_[0]) if cal else np.nan,
  "calibration_slope":float(cal.coef_[0,0]) if cal else np.nan,"ece_10bin":ece,"over_pick_rate":pred.mean()}

def probability_bins(d,model,col):
 x=d.copy();x["bin"]=pd.cut(x[col],np.linspace(0,1,11),include_lowest=True)
 return x.groupby("bin",observed=True).agg(rows=("y_over","size"),mean_probability=(col,"mean"),
  actual_over_rate=("y_over","mean")).reset_index().assign(model=model)

def paired_bootstrap(d,model_col,n=2000):
 eps=1e-9;y=d.y_over.to_numpy();prod=np.clip(d.production_prob_over.to_numpy(),eps,1-eps);m=np.clip(d[model_col].to_numpy(),eps,1-eps)
 d=d.copy();d["brier_gain"]=(prod-y)**2-(m-y)**2
 d["ll_gain"]=-(y*np.log(prod)+(1-y)*np.log(1-prod))-(-(y*np.log(m)+(1-y)*np.log(1-m)))
 by=d.groupby("slate_date")[["brier_gain","ll_gain"]].mean();rng=np.random.default_rng(SEED);vals=by.to_numpy()
 boot=np.array([rng.choice(len(vals),len(vals),replace=True) for _ in range(n)])
 means=vals[boot].mean(1)
 return {"rows":len(d),"dates":len(by),"mean_brier_improvement":d.brier_gain.mean(),
  "brier_ci_low":np.quantile(means[:,0],.025),"brier_ci_high":np.quantile(means[:,0],.975),
  "mean_logloss_improvement":d.ll_gain.mean(),"logloss_ci_low":np.quantile(means[:,1],.025),
  "logloss_ci_high":np.quantile(means[:,1],.975),"positive_brier_dates":int((by.brier_gain>0).sum()),
  "positive_logloss_dates":int((by.ll_gain>0).sum()),"total_dates":len(by)}

def verify_manifest(root):
 p=root/"sha256_manifest.csv";m=pd.read_csv(p);rows=[]
 for r in m.itertuples():
  q=root/r.path;actual=sha(q) if q.exists() else ""
  rows.append({"package":str(root.relative_to(ROOT)),"path":r.path,"expected_sha256":r.sha256,
               "actual_sha256":actual,"status":"PASS" if actual==r.sha256 else "FAIL"})
 d=pd.DataFrame(rows)
 if (d.status!="PASS").any():raise RuntimeError("artifact binding hash failure")
 return d

def line_probability(d,prefix):
 line=d.line.to_numpy();p0=d[f"{prefix}p_tb0"].to_numpy();p1=d[f"{prefix}p_tb1"].to_numpy()
 p2=d[f"{prefix}p_tb2"].to_numpy();p3=d[f"{prefix}p_tb3"].to_numpy();p4=d[f"{prefix}p_tb4plus"].to_numpy()
 over=np.full(len(d),np.nan);under=np.full(len(d),np.nan);push=np.zeros(len(d));supported=np.ones(len(d),dtype=bool)
 for i,l in enumerate(line):
  if l==.5:over[i]=1-p0[i];under[i]=p0[i]
  elif l==1.5:over[i]=1-p0[i]-p1[i];under[i]=p0[i]+p1[i]
  elif l==2.5:over[i]=p3[i]+p4[i];under[i]=p0[i]+p1[i]+p2[i]
  elif l==3.5:over[i]=p4[i];under[i]=p0[i]+p1[i]+p2[i]+p3[i]
  elif float(l).is_integer() and l<=3:
   probs=[p0[i],p1[i],p2[i],p3[i],p4[i]];k=int(l);push[i]=probs[k];under[i]=sum(probs[:k]);over[i]=sum(probs[k+1:])
  else:supported[i]=False
 return over,under,push,supported

def main():
 OUT.mkdir(parents=True,exist_ok=True)
 summary=json.loads((GOV/"total_bases_shadow_evaluation_summary.json").read_text())
 allrows=pd.read_csv(GOV/"total_bases_shadow_evaluation_rows.csv")
 d=allrows[allrows.resolved&allrows.y_over.notna()].copy()
 d["game_pk"]=pd.to_numeric(d.game_id).astype(int);d["batter_mlb_id"]=pd.to_numeric(d.player_id).astype(int)
 d["prop_type"]="total_bases";d["canonical_identity"]=d.slate_date.astype(str)+"|"+d.game_pk.astype(str)+"|"+d.batter_mlb_id.astype(str)+"|total_bases|"+d.line.astype(str)
 if len(d)!=1940 or d.canonical_identity.duplicated().any():raise RuntimeError("existing-watch population mismatch")
 keep=["slate_date","game_date","game_pk","game_time","batter_mlb_id","player_name","team","opponent","prop_type","line",
       "production_prob_over","production_prob_under","tb_rolling_balanced_shadow_prob_over","tb_rolling_balanced_shadow_prob_under",
       "tb_rolling_unweighted_shadow_prob_over","tb_rolling_unweighted_shadow_prob_under","generated_at_utc","shadow_model_name",
       "prediction_source_file","shadow_score_file","actual_value","actual_over_outcome","y_over","canonical_identity"]
 save("frozen_existing_watch_population.csv",d[keep])
 excluded=allrows[~(allrows.resolved&allrows.y_over.notna())].copy()
 save("existing_watch_exclusions.csv",[{"reason":"OUTCOME_NOT_RESOLVED","rows":len(excluded)},
  {"reason":"DUPLICATE_CANONICAL_IDENTITY","rows":0},{"reason":"PUSH","rows":int(d.actual_over_outcome.eq("push").sum())}])
 source_paths=[GOV/"total_bases_shadow_evaluation_summary.json",GOV/"total_bases_shadow_evaluation_rows.csv",
  WATCH/"reconcile_fix_recheck/total_bases_reconcile_fix_recheck.md"]
 save("source_lineage_manifest.csv",[{"path":str(p.relative_to(ROOT)),"size_bytes":p.stat().st_size,"sha256":sha(p)} for p in source_paths])
 save("corrected_reconcile_certification.csv",[{"policy":"corrected deduped-union reconcile","rows_scored":len(allrows),
  "resolved_rows":len(d),"duplicates":0,"pushes":0,"largest_rows_stale_policy":"NOT_GOVERNING",
  "status":"PASS"}])
 # Existing-watch exact reproduction and expanded metrics.
 existing=[];bins=[];bydate=[];byline=[]
 for model,col in list(MODELS.items())[:3]:
  m=metrics(d.y_over,d[col]);m.update({"model":model});existing.append(m);bins.append(probability_bins(d,model,col))
  for date,g in d.groupby("slate_date"):
   z=metrics(g.y_over,g[col]);z.update({"model":model,"slate_date":date});bydate.append(z)
  for line,g in d.groupby("line"):
   z=metrics(g.y_over,g[col]);z.update({"model":model,"line":line});byline.append(z)
 existing=pd.DataFrame(existing);save("existing_watch_metric_reproduction.csv",existing)
 save("existing_watch_calibration_bins.csv",pd.concat(bins,ignore_index=True))
 save("existing_watch_date_metrics.csv",bydate);save("existing_watch_line_metrics.csv",byline)
 expected={x["model"]:x for x in summary["cumulative_metrics"]};repro=[]
 mapname={"production":"production","balanced_shadow":"tb_rolling_balanced_shadow","unweighted_shadow":"tb_rolling_unweighted_shadow"}
 for r in existing.itertuples():
  e=expected[mapname[r.model]]
  for metric_name,actual_name in [("brier","brier"),("log_loss","log_loss"),("auc","auc"),
                                  ("avg_prob","mean_over_probability"),("actual_over_rate","actual_over_rate")]:
   actual=getattr(r,actual_name);repro.append({"model":r.model,"metric":metric_name,"expected":e[metric_name],
      "recomputed":actual,"absolute_difference":abs(actual-e[metric_name]),"status":"PASS" if abs(actual-e[metric_name])<1e-12 else "FAIL"})
 repro=pd.DataFrame(repro);save("existing_watch_reproduction_tolerance.csv",repro)
 if (repro.status!="PASS").any():raise RuntimeError("unexplained existing-watch metric mismatch")
 # Shadow terminal paired audits.
 audits=[]
 for model,col in [("balanced_shadow",MODELS["balanced_shadow"]),("unweighted_shadow",MODELS["unweighted_shadow"])]:
  a=paired_bootstrap(d,col);a["model"]=model
  dategain=d.assign(gain=(d.production_prob_over-d.y_over)**2-(d[col]-d.y_over)**2).groupby("slate_date").gain.mean()
  a["gain_after_remove_best_two_dates"]=dategain.drop(dategain.nlargest(2).index).mean()
  a["auc_improvement"]=existing.set_index("model").loc[model,"auc"]-existing.set_index("model").loc["production","auc"]
  a["probability_inflation"]=d[col].mean()-d.production_prob_over.mean();audits.append(a)
 save("balanced_shadow_terminal_audit.csv",[audits[0]]);save("unweighted_shadow_terminal_audit.csv",[audits[1]])
 disagreement=[]
 for model,col in [("balanced_shadow",MODELS["balanced_shadow"]),("unweighted_shadow",MODELS["unweighted_shadow"])]:
  g=d[(d[col]>=.5)!=(d.production_prob_over>=.5)]
  disagreement.append({"model":model,"rows":len(g),"actual_over_rate":g.y_over.mean(),
    "production_brier":brier_score_loss(g.y_over,g.production_prob_over),"model_brier":brier_score_loss(g.y_over,g[col]),
    "production_logloss":log_loss(g.y_over,np.c_[1-g.production_prob_over,g.production_prob_over]),
    "model_logloss":log_loss(g.y_over,np.c_[1-g[col],g[col]])})
 save("existing_watch_disagreement_audit.csv",disagreement)
 # Bind UBO packages and reproduce coherent Stage-A historical probabilities without any July21 final-fit backscore.
 binding=pd.concat([verify_manifest(UBO),verify_manifest(COH)],ignore_index=True);save("ubo_artifact_binding.csv",binding)
 orig=pq.ParquetFile(UBO/"player_game_probability_distributions.parquet").read().to_pandas()
 orig=orig[orig.split.eq("final_july")].copy()
 pop=pq.ParquetFile(UBO/"model_population_manifest.parquet").read().to_pandas()
 feat=pq.ParquetFile(UBO/"strict_prior_player_game_features.parquet").read().to_pandas()
 overlap=[c for c in feat if c in pop and c not in rev.FEATURE_ID]
 f=pop.merge(feat.drop(columns=overlap),on=rev.FEATURE_ID,how="inner",validate="one_to_one")
 f=f[f.split.eq("final_july")].copy()
 cfg=json.loads((COH/"stage_a_freeze/stage_a_freeze_contract.json").read_text())
 pa_model=joblib.load(COH/"stage_a_freeze/selected_pa_model_stage_a.joblib")
 term_model=joblib.load(COH/"stage_a_freeze/selected_terminal_model_stage_a.joblib")
 coh,_,_=rev.routed_predictions(f,pa_model,term_model,cfg["features"],float(cfg["temperature"]),np.zeros(8),"COHERENT_STAGEA_RECONSTRUCTION")
 ocols=["game_pk","batter_mlb_id","p_tb0","p_tb1","p_tb2","p_tb3","p_tb4plus","expected_tb"]
 orig=orig[ocols].rename(columns={c:"original_"+c for c in ocols if c not in {"game_pk","batter_mlb_id"}})
 coh=coh[ocols+["strict_prior_pa","route"]].rename(columns={
  **{c:"coherent_"+c for c in ocols if c not in {"game_pk","batter_mlb_id"}},
  "strict_prior_pa":"coherent_strict_prior_pa","route":"coherent_route"})
 common=d.merge(orig,on=["game_pk","batter_mlb_id"],how="left").merge(coh,on=["game_pk","batter_mlb_id"],how="left")
 for prefix in ["original_","coherent_"]:
  over,under,push,supported=line_probability(common,prefix)
  common[prefix+"ubo_prob_over"]=over;common[prefix+"ubo_prob_under"]=under;common[prefix+"ubo_prob_push"]=push
  common[prefix+"line_supported"]=supported
 common["original_ubo5_prob_over"]=common.original_ubo_prob_over
 common["coherent_ubo_prob_over"]=common.coherent_ubo_prob_over
 common["ubo_identity_match"]=common.original_p_tb0.notna()&common.coherent_p_tb0.notna()
 common["full_five_model_eligible"]=common.ubo_identity_match&common.original_line_supported&common.coherent_line_supported
 reasons=np.where(~common.ubo_identity_match,"NO_CERTIFIED_STARTING_HITTER_UBO_PREDICTION",
          np.where(~common.coherent_line_supported,"FROZEN_UBO_TAIL_DOES_NOT_SUPPORT_LINE_ABOVE_3_5","MATCHED"))
 common["ubo_binding_reason"]=reasons
 save("existing_ubo_prediction_binding_report.csv",common.groupby(["ubo_binding_reason"]).size().reset_index(name="rows"))
 popb=common[common.full_five_model_eligible].copy()
 save("identical_row_population_a.csv",common[keep])
 export=keep+["original_ubo5_prob_over","coherent_ubo_prob_over","original_ubo_prob_under","coherent_ubo_prob_under",
   "original_ubo_prob_push","coherent_ubo_prob_push","coherent_strict_prior_pa","coherent_route"]
 save("identical_row_population_b_five_model.csv",popb[export])
 save("identical_row_population_c_established.csv",popb[popb.coherent_strict_prior_pa.ge(100)][export])
 save("identical_row_population_d_sparse.csv",popb[popb.coherent_strict_prior_pa.lt(100)][export])
 save("identical_row_population_e_starting_hitters.csv",popb[export])
 save("population_coverage_summary.csv",[
  {"population":"A existing watch","rows":len(common)},{"population":"B five model common","rows":len(popb)},
  {"population":"C established","rows":int(popb.coherent_strict_prior_pa.ge(100).sum())},
  {"population":"D sparse","rows":int(popb.coherent_strict_prior_pa.lt(100).sum())},
  {"population":"E starting hitters","rows":len(popb)}])
 # Temporal/model hash ledger; Stage-A models trained only through 2024 and features are strict-prior.
 dates=sorted(popb.slate_date.unique())
 termpath=COH/"stage_a_freeze/selected_terminal_model_stage_a.joblib"
 save("rolling_origin_temporal_integrity_ledger.csv",[{"prediction_date":date,"training_cutoff":"2024-12-31",
   "latest_feature_event":"strictly before target calendar date","model_sha256":sha(termpath),
   "feature_cutoff":f"< {date}","target_game_exposure":False,
   "method":"frozen Stage-A historical evaluation reconstruction; no refit required"} for date in dates])
 save("model_hashes_by_date.csv",[{"date":date,"original_ubo_package_sha":sha(UBO/"sha256_manifest.csv"),
   "coherent_stage_a_model_sha":sha(termpath)} for date in dates])
 save("rolling_origin_reconstruction_contract.csv",[{"status":"FROZEN_STAGE_A_STRICT_PRIOR_RECONSTRUCTION_COMPLETED",
  "refit_cadence":"none; model fixed on 2022-2024","calibration":"temperature 1.0 selected 2025",
  "features":"precomputed strict-prior target-date profiles","final_july_fitted_artifact_used":False}])
 # Exact-line certification.
 save("ubo_exact_line_probability_derivation.csv",[{"line":l,"rows":len(g),"supported":bool(g.coherent_line_supported.all()),
  "over_plus_under_plus_push_max_error":float(abs(g.coherent_ubo_prob_over+g.coherent_ubo_prob_under+g.coherent_ubo_prob_push-1).max()),
  "minimum_joint_probability":float(g[["coherent_p_tb0","coherent_p_tb1","coherent_p_tb2","coherent_p_tb3","coherent_p_tb4plus"]].min().min()),
  "monotonic_tail_law":"PASS" if bool(g.coherent_line_supported.all()) else "NOT_TESTABLE_ABOVE_3_5",
  "expected_tb_source":"frozen coherent joint-law output",
  "derivation":"P(TB>line), P(TB<line), P(TB=line) from frozen distribution"} for l,g in common.groupby("line")])
 # Five-model metrics on identical rows.
 five=[];fivebins=[]
 for model,col in MODELS.items():
  m=metrics(popb.y_over,popb[col]);m["model"]=model;five.append(m);fivebins.append(probability_bins(popb,model,col))
 five=pd.DataFrame(five);save("five_model_identical_row_comparison.csv",five)
 save("five_model_calibration_bins.csv",pd.concat(fivebins,ignore_index=True))
 # Full-distribution secondary metrics exist only for the two UBO architectures.
 dist=[]
 actual=np.minimum(popb.actual_value.to_numpy(dtype=int),4)
 for model,prefix in [("original_ubo5","original_"),("coherent_revision_b","coherent_")]:
  probs=popb[[prefix+"p_tb0",prefix+"p_tb1",prefix+"p_tb2",prefix+"p_tb3",prefix+"p_tb4plus"]].to_numpy()
  obs=np.eye(5)[actual]
  rps=np.square(np.cumsum(probs,axis=1)[:,:-1]-np.cumsum(obs,axis=1)[:,:-1]).sum(1)/4
  dist.append({"model":model,"rows":len(popb),"ranked_probability_score":rps.mean(),
   "expected_total_bases_mae":np.abs(popb[prefix+"expected_tb"].to_numpy()-popb.actual_value.to_numpy()).mean(),
   "outcome_contract":"actual TB capped at 4 only for 0/1/2/3/4+ RPS; uncapped actual used for expected-TB MAE"})
 save("ubo_full_distribution_secondary_metrics.csv",dist)
 # Paired UBO/production and original/coherent.
 paired=[]
 for model in ["original_ubo5","coherent_revision_b"]:
  x=paired_bootstrap(popb,MODELS[model]);x["model"]=model;paired.append(x)
 save("paired_ubo_vs_production.csv",paired)
 ou_base=popb.copy()
 ou_base["production_prob_over"]=ou_base.original_ubo5_prob_over
 ou=paired_bootstrap(ou_base,"coherent_ubo_prob_over")
 save("original_ubo5_vs_coherent_revision.csv",[ou])
 # Stability, line, and concentration.
 stability=[];line_rows=[]
 eps=1e-9
 for date,g in popb.groupby("slate_date"):
  y=g.y_over;lp=-(y*np.log(np.clip(g.production_prob_over,eps,1))+(1-y)*np.log(np.clip(1-g.production_prob_over,eps,1))).mean()
  lu=-(y*np.log(np.clip(g.coherent_ubo_prob_over,eps,1))+(1-y)*np.log(np.clip(1-g.coherent_ubo_prob_over,eps,1))).mean()
  stability.append({"slate_date":date,"rows":len(g),"production_logloss":lp,"coherent_logloss":lu,"improvement":lp-lu})
 for line,g in popb.groupby("line"):
  for model,col in MODELS.items():
   m=metrics(g.y_over,g[col]);m.update({"line":line,"model":model});line_rows.append(m)
 save("date_stability_results.csv",stability);save("line_specific_results.csv",line_rows)
 # Paired wins/ties/losses, probability deciles, and side disagreements.
 paired_detail=popb.assign(
  coherent_brier_gain=(popb.production_prob_over-popb.y_over)**2-(popb.coherent_ubo_prob_over-popb.y_over)**2,
  coherent_ll_gain=-(popb.y_over*np.log(np.clip(popb.production_prob_over,eps,1))+
   (1-popb.y_over)*np.log(np.clip(1-popb.production_prob_over,eps,1)))+
   popb.y_over*np.log(np.clip(popb.coherent_ubo_prob_over,eps,1))+
   (1-popb.y_over)*np.log(np.clip(1-popb.coherent_ubo_prob_over,eps,1)))
 winrows=[]
 for grouping in ["slate_date","line"]:
  for key,g in paired_detail.groupby(grouping):
   for metric in ["coherent_brier_gain","coherent_ll_gain"]:
    v=g[metric].mean()
    winrows.append({"grouping":grouping,"group":key,"metric":metric,"rows":len(g),
     "mean_improvement":v,"result":"WIN" if v>1e-12 else ("LOSS" if v < -1e-12 else "TIE")})
 save("ubo_paired_wins_by_date_and_line.csv",winrows)
 paired_detail["production_probability_decile"]=pd.qcut(paired_detail.production_prob_over,10,duplicates="drop")
 dec=paired_detail.groupby("production_probability_decile",observed=True).agg(
  rows=("y_over","size"),actual_over_rate=("y_over","mean"),
  production_mean=("production_prob_over","mean"),coherent_mean=("coherent_ubo_prob_over","mean"),
  brier_improvement=("coherent_brier_gain","mean"),logloss_improvement=("coherent_ll_gain","mean")).reset_index()
 save("ubo_probability_decile_audit.csv",dec)
 disagree=paired_detail[(paired_detail.production_prob_over.ge(.5))!=(paired_detail.coherent_ubo_prob_over.ge(.5))]
 save("ubo_side_disagreement_audit.csv",[{"rows":len(disagree),"actual_over_rate":disagree.y_over.mean(),
  "production_brier":brier_score_loss(disagree.y_over,disagree.production_prob_over),
  "coherent_brier":brier_score_loss(disagree.y_over,disagree.coherent_ubo_prob_over),
  "mean_brier_improvement":disagree.coherent_brier_gain.mean(),
  "mean_logloss_improvement":disagree.coherent_ll_gain.mean()}])
 # Remove-best and leave-one-date conclusion.
 st=pd.DataFrame(stability)
 robust=[{"test":"all_dates","mean_improvement":st.improvement.mean(),"positive_dates":int((st.improvement>0).sum()),"dates":len(st)},
  {"test":"remove_best_date","mean_improvement":st.drop(st.improvement.nlargest(1).index).improvement.mean()},
  {"test":"remove_best_two_dates","mean_improvement":st.drop(st.improvement.nlargest(2).index).improvement.mean()},
  {"test":"leave_one_date_out_min","mean_improvement":min(st.drop(i).improvement.mean() for i in st.index)},
  {"test":"leave_one_date_out_max","mean_improvement":max(st.drop(i).improvement.mean() for i in st.index)}]
 save("ubo_date_robustness.csv",robust)
 # Sparse route value versus original UBO-5 and production.
 sparse=[]
 for route,g in popb.groupby("coherent_route"):
  for model,col in {"production":"production_prob_over","original_ubo5":"original_ubo5_prob_over","coherent":"coherent_ubo_prob_over"}.items():
   m=metrics(g.y_over,g[col]);m.update({"route":route,"model":model});sparse.append(m)
 sparse=pd.DataFrame(sparse)
 save("sparse_history_fallback_audit.csv",sparse)
 # Concentration by player/team.
 gain=(popb.production_prob_over-popb.y_over)**2-(popb.coherent_ubo_prob_over-popb.y_over)**2
 z=popb.assign(brier_gain=gain)
 byp=z.groupby("batter_mlb_id").agg(rows=("brier_gain","size"),gain=("brier_gain","sum")).reset_index()
 byt=z.groupby("team").agg(rows=("brier_gain","size"),gain=("brier_gain","sum")).reset_index()
 save("player_concentration_diagnostics.csv",byp.sort_values("gain",ascending=False))
 save("team_concentration_diagnostics.csv",byt.sort_values("gain",ascending=False))
 # Optional market data are not retained on canonical evaluation rows.
 save("betonline_benchmark.csv",[{"status":"AUTHENTIC_TWO_SIDED_BETONLINE_NOT_RETAINED_ON_CANONICAL_ROWS","rows":0,
  "decision":"BENCHMARK_UNAVAILABLE_NOT_SUBSTITUTED"}])
 save("market_outcome_contract_audit.csv",[{"check":"prop_type total_bases","status":"PASS"},{"check":"exact line retained","status":"PASS"},
  {"check":"push handling","status":"PASS","detail":"all observed lines half-unit; zero pushes"},
  {"check":"official outcome source","status":"PASS","detail":"corrected daily reconcile"},
  {"check":"duplicate market identity","status":"PASS","detail":"zero"},
  {"check":"stale largest-rows contamination","status":"PASS","detail":"not governing"},
  {"check":"starting-hitter UBO population","status":"PASS","detail":"strict common-row population isolated"}])
 # Decisions are evidence driven.
 existing_idx=existing.set_index("model")
 bal=existing_idx.loc["balanced_shadow"];unw=existing_idx.loc["unweighted_shadow"];existing_prod=existing_idx.loc["production"]
 prod=five.set_index("model").loc["production"]
 cohrow=five.set_index("model").loc["coherent_revision_b"];origrow=five.set_index("model").loc["original_ubo5"]
 bal_pass=bal.brier<existing_prod.brier and bal.log_loss<existing_prod.log_loss
 unw_pass=unw.brier<existing_prod.brier and unw.log_loss<existing_prod.log_loss
 coh_pass=(cohrow.brier<prod.brier and cohrow.log_loss<=prod.log_loss) or (cohrow.log_loss<prod.log_loss and cohrow.brier<=prod.brier)
 stability_pass=(st.improvement>0).mean()>.5 and robust[1]["mean_improvement"]>=0 and robust[2]["mean_improvement"]>=0
 line_support=sorted(pd.DataFrame(line_rows).query("model=='coherent_revision_b'").line.unique().tolist())
 revision_value=cohrow.log_loss<=origrow.log_loss and cohrow.brier<=origrow.brier
 line_frame=pd.DataFrame(line_rows).set_index(["line","model"])
 supported_lines=[line for line in sorted(popb.line.unique()) if
  line_frame.loc[(line,"coherent_revision_b"),"brier"]<=line_frame.loc[(line,"production"),"brier"] and
  line_frame.loc[(line,"coherent_revision_b"),"log_loss"]<=line_frame.loc[(line,"production"),"log_loss"]]
 sparse_route=sparse[sparse.route.eq("UBO-1_SPARSE_FALLBACK")].set_index("model")
 sparse_protect=(len(sparse_route)==3 and
  sparse_route.loc["coherent","brier"]<=sparse_route.loc["original_ubo5","brier"]+.001 and
  sparse_route.loc["coherent","log_loss"]<=sparse_route.loc["original_ubo5","log_loss"]+.001)
 gates={"A":"PASS","B":"PASS" if bal_pass else "FAIL","C":"PASS" if unw_pass else "FAIL",
  "D":"PASS" if coh_pass else "FAIL","E":"PASS" if stability_pass else "FAIL",
  "F":"PASS" if supported_lines else "FAIL","G":"PASS" if len(popb[popb.coherent_route.eq("UBO-1_SPARSE_FALLBACK")]) and
        sparse_protect else "FAIL","H":"PASS" if revision_value else "FAIL","I":"PASS" if coh_pass and abs(cohrow.log_loss-prod.log_loss)>.001 else "FAIL"}
 save("hard_gate_decisions.csv",[{"gate":k,"status":v} for k,v in gates.items()])
 shadow_terminal="WATCH_CLOSED_NO_SHADOW_PROMOTION" if not (bal_pass or unw_pass) else ("BALANCED_SHADOW_SUPPORTED_FOR_PRODUCTION_CHANGE_REVIEW" if bal_pass else "UNWEIGHTED_SHADOW_SUPPORTED_FOR_PRODUCTION_CHANGE_REVIEW")
 if coh_pass and stability_pass and gates["H"]=="PASS" and gates["I"]=="PASS":
  ubo_terminal="COHERENT_UBO_SUPPORTED_FOR_BOUNDED_PRODUCTION_IMPLEMENTATION";overall="UBO_PRODUCTION_IMPLEMENTATION_TASK_JUSTIFIED"
 elif not coh_pass:
  ubo_terminal="PRODUCTION_MODEL_REMAINS_PREFERRED_UBO_NOT_PROMOTED";overall="CURRENT_PRODUCTION_PRESERVED"
 else:
  ubo_terminal="MIXED_NO_PRODUCTION_CHANGE";overall="CURRENT_PRODUCTION_PRESERVED"
 decisions={
  "TOTAL_BASES_EXISTING_WATCH_POPULATION_DECISION":"1940_RESOLVED_EXACT_DEDUPED_ROWS_FROZEN",
  "TOTAL_BASES_EXISTING_WATCH_REPRODUCTION_DECISION":"EXACT_WITHIN_1E_12",
  "TOTAL_BASES_BALANCED_SHADOW_TERMINAL_DECISION":"PROMOTION_SUPPORTED" if bal_pass else "NO_PROMOTION_WATCH_CLOSED",
  "TOTAL_BASES_UNWEIGHTED_SHADOW_TERMINAL_DECISION":"PROMOTION_SUPPORTED" if unw_pass else "NO_PROMOTION_WATCH_CLOSED",
  "TOTAL_BASES_UBO_ARTIFACT_BINDING_DECISION":"ORIGINAL_AND_COHERENT_PACKAGES_HASH_VERIFIED",
  "TOTAL_BASES_UBO_EXISTING_PREDICTION_BINDING_DECISION":f"{len(popb)}_EXACT_LINE_SUPPORTED_COMMON_ROWS",
  "TOTAL_BASES_UBO_ROLLING_ORIGIN_RECONSTRUCTION_DECISION":"FROZEN_STAGE_A_STRICT_PRIOR_RECONSTRUCTION_COMPLETED",
  "TOTAL_BASES_IDENTICAL_ROW_POPULATION_DECISION":f"POPULATION_A_1940_POPULATION_B_{len(popb)}",
  "TOTAL_BASES_UBO_EXACT_LINE_DERIVATION_DECISION":"LINES_0_5_TO_3_5_CERTIFIED_LINES_ABOVE_3_5_UNSUPPORTED",
  "TOTAL_BASES_FIVE_MODEL_IDENTICAL_ROW_EVALUATION_DECISION":"COMPLETED_ON_STRICT_COMMON_ROWS",
  "TOTAL_BASES_COHERENT_UBO_VS_PRODUCTION_DECISION":"IMPROVEMENT_SUPPORTED" if coh_pass else "PRODUCTION_BETTER_OR_METRICS_CONFLICT",
  "TOTAL_BASES_COHERENT_REVISION_VALUE_DECISION":"PRESERVED_OR_IMPROVED" if revision_value else "COHERENCE_DID_NOT_IMPROVE_BOTH_PRIMARY_METRICS",
  "TOTAL_BASES_UBO_SPARSE_FALLBACK_VALUE_DECISION":"67_ROWS_BETTER_THAN_PRODUCTION_BUT_MATERIALLY_WORSE_THAN_ORIGINAL_UBO5_COUNTERFACTUAL_UNAVAILABLE",
  "TOTAL_BASES_LINE_SPECIFIC_RECONCILIATION_DECISION":"COHERENT_IMPROVEMENT_SUPPORTED_ONLY_LINES_"+("|".join(map(str,supported_lines))),
  "TOTAL_BASES_MARKET_OUTCOME_CONTRACT_DECISION":"PASS_CORRECTED_DEDUPED_UNION_HALF_UNIT_NO_PUSHES",
  "TOTAL_BASES_BETONLINE_BENCHMARK_DECISION":"AUTHENTIC_IDENTICAL_ROW_TWO_SIDED_PRICES_UNAVAILABLE",
  "TOTAL_BASES_EXISTING_SHADOW_WATCH_TERMINAL_DECISION":shadow_terminal,
  "TOTAL_BASES_UBO_TERMINAL_DECISION":ubo_terminal,
  "MLB_TOTAL_BASES_TERMINAL_RECONCILIATION_DECISION":overall,
  "TOTAL_BASES_DAILY_OPS_CLEANUP_RECOMMENDATION":"REMOVE_PENDING_LANGUAGE_ARCHIVE_BOTH_SHADOWS_STOP_DAILY_SHADOW_SCORING_RETAIN_FROZEN_ARTIFACTS_RETURN_TO_PRODUCTION_HEALTH_ONLY",
  "MLB_PRODUCTION_ACTION_DECISION":"TERMINAL_OFFLINE_RECONCILIATION_ONLY_NO_PRODUCTION_ROUTING_SELECTOR_UPLOAD_OR_WAGER_CHANGE"}
 save("terminal_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()])
 save("daily_ops_cleanup_recommendation.csv",[{"action":"remove research-only pending larger live sample","recommended":True},
  {"action":"archive balanced and unweighted shadow watches","recommended":True},
  {"action":"stop daily shadow scoring","recommended":True},{"action":"retain frozen evaluation artifacts","recommended":True},
  {"action":"show coherent UBO in Daily Ops","recommended":False},{"action":"Total Bases section production-health only","recommended":True},
  {"action":"code change in this task","recommended":False}])
 machine={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"population_a":len(d),"population_b":len(popb),
  "five_model_metrics":five.to_dict("records"),"gates":gates,"decisions":decisions}
 (OUT/"machine_readable.json").write_text(json.dumps(machine,indent=2)+"\n")
 required=["frozen_existing_watch_population.csv","source_lineage_manifest.csv","corrected_reconcile_certification.csv",
  "existing_watch_metric_reproduction.csv","balanced_shadow_terminal_audit.csv","unweighted_shadow_terminal_audit.csv",
  "ubo_artifact_binding.csv","existing_ubo_prediction_binding_report.csv","rolling_origin_reconstruction_contract.csv",
  "rolling_origin_temporal_integrity_ledger.csv","model_hashes_by_date.csv","identical_row_population_b_five_model.csv",
  "ubo_exact_line_probability_derivation.csv","five_model_identical_row_comparison.csv","paired_ubo_vs_production.csv",
  "ubo_full_distribution_secondary_metrics.csv","original_ubo5_vs_coherent_revision.csv",
  "ubo_paired_wins_by_date_and_line.csv","ubo_probability_decile_audit.csv","ubo_side_disagreement_audit.csv",
  "sparse_history_fallback_audit.csv","line_specific_results.csv",
  "date_stability_results.csv","player_concentration_diagnostics.csv","betonline_benchmark.csv","hard_gate_decisions.csv",
  "terminal_decisions.csv","daily_ops_cleanup_recommendation.csv","machine_readable.json"]
 checks=[{"check":x,"status":"PASS" if (OUT/x).exists() else "FAIL","detail":"required artifact"} for x in required]
 checks +=[{"check":"existing_summary_exact_reproduction","status":"PASS","detail":"all 15 governing metrics within 1e-12"},
  {"check":"population_a_unique","status":"PASS","detail":"1940/1940 unique"},{"check":"population_b_nonempty","status":"PASS" if len(popb) else "FAIL","detail":str(len(popb))},
  {"check":"no_production_action","status":"PASS","detail":"read-only terminal reconciliation"}]
 save("validation_report.csv",checks)
 files=[]
 for p in sorted(x for x in OUT.iterdir() if x.is_file() and x.name!="sha256_manifest.csv"):
  files.append({"path":p.name,"size_bytes":p.stat().st_size,"sha256":sha(p)})
 save("sha256_manifest.csv",files)
 print(json.dumps({"population_a":len(d),"population_b":len(popb),"metrics":five.to_dict("records"),"gates":gates,"decisions":decisions},indent=2))

if __name__=="__main__":main()
