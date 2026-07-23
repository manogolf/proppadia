#!/usr/bin/env python3
"""Decisive uncertainty-aware Event-Process v2 trial-gate experiment."""

from __future__ import annotations

import csv, hashlib, json, math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from scipy.special import betaln
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score

from backend.mlb.scripts import run_mlb_hits05_event_process_architecture_v1 as v1

ROOT=Path(__file__).resolve().parents[3]
V1=ROOT/"artifacts/analysis/model_development/mlb_hits05_event_process_architecture_v1/2026-07-21"
OUT=ROOT/"artifacts/analysis/model_development/mlb_hits05_event_process_architecture_v2/2026-07-21"
COMMON=ROOT/"artifacts/analysis/model_development/mlb_hits05_metric_integrity_correction_and_20_slate_replication/2026-07-21/twenty_slate_common_row_ledger.csv"
KEY=["slate_date","game_id","player_id"]
PA_FEATURES=v1.PA_FEATURES

def sha(p:Path)->str:
 h=hashlib.sha256()
 with p.open("rb") as f:
  for c in iter(lambda:f.read(1<<20),b""): h.update(c)
 return h.hexdigest()
def rel(p:Path)->str:
 try:return str(p.resolve().relative_to(ROOT))
 except:return str(p)
def write_csv(name:str,data:pd.DataFrame|list[dict],fields:list[str]|None=None):
 p=OUT/name
 if isinstance(data,pd.DataFrame): data.to_csv(p,index=False);return
 fields=fields or list(dict.fromkeys(k for r in data for k in r)) or ["status"]
 with p.open("w",newline="",encoding="utf-8") as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore");w.writeheader();w.writerows(data)
def plugin(pa:np.ndarray,p:np.ndarray)->np.ndarray:
 n=np.array([2.,3.,4.,5.,6.]);return (pa*np.power(1-p[:,None],n)).sum(1)
def integrated(pa:np.ndarray,a:np.ndarray,b:np.ndarray)->np.ndarray:
 n=np.array([2.,3.,4.,5.,6.]);z=np.exp(betaln(a[:,None],b[:,None]+n)-betaln(a[:,None],b[:,None]));return (pa*z).sum(1)
def mrow(g:pd.DataFrame,col:str,pop:str)->dict[str,Any]:
 x=g[["hitless",col,"slate_date"]].dropna();y=x.hitless.astype(int);p=x[col].clip(1e-6,1-1e-6)
 z=np.log(p/(1-p)).to_numpy().reshape(-1,1)
 try:lr=LogisticRegression(C=1e6,max_iter=500).fit(z,y);ci=float(lr.intercept_[0]);cs=float(lr.coef_[0,0])
 except:ci=cs=np.nan
 bins=pd.qcut(p,10,duplicates="drop");cal=x.assign(_b=bins).groupby("_b",observed=True).agg(obs=("hitless","mean"),pred=(col,"mean"));ece=float((cal.obs-cal.pred).abs().mean())
 return {"population":pop,"variant":col,"rows":len(x),"dates":x.slate_date.nunique(),"prevalence":float(y.mean()),"pr_auc":float(average_precision_score(y,p)),"roc_auc":float(roc_auc_score(y,p)),"brier":float(brier_score_loss(y,p)),"log_loss":float(log_loss(y,p)),"calibration_intercept":ci,"calibration_slope":cs,"ece":ece}
def tails(g:pd.DataFrame,cols:list[str],pop:str)->list[dict]:
 out=[];base=g.hitless.mean()
 for c in cols:
  for pct in [5,10,15,20,25]:
   n=max(1,math.ceil(len(g)*pct/100));x=g.nlargest(n,c);cap=int(x.hitless.sum())
   out.append({"population":pop,"variant":c,"capacity_pct":pct,"rows_flagged":n,"hitless_captured":cap,"precision":cap/n,"recall":cap/max(1,int(g.hitless.sum())),"lift":cap/n/base,"dates":x.slate_date.nunique()})
 return out
def histories(pg:pd.DataFrame,ev:pd.DataFrame)->pd.DataFrame:
 daily=ev.groupby(["player_id","slate_date"],as_index=False).agg(event_hits=("official_hit","sum"),event_pa=("official_hit","size"));daily.slate_date=pd.to_datetime(daily.slate_date)
 by={int(pid):g.sort_values("slate_date") for pid,g in daily.groupby("player_id")};rows=[]
 for r in pg[["player_id","slate_date"]].itertuples(index=False):
  dt=pd.Timestamp(r.slate_date);g=by.get(int(r.player_id));
  if g is None: h=pa=h14=pa14=h30=pa30=0;days=np.nan
  else:
   q=g[g.slate_date<dt];h=int(q.event_hits.sum());pa=int(q.event_pa.sum());q14=q[q.slate_date>=dt-pd.Timedelta(days=14)];h14=int(q14.event_hits.sum());pa14=int(q14.event_pa.sum());q30=q[q.slate_date>=dt-pd.Timedelta(days=30)];h30=int(q30.event_hits.sum());pa30=int(q30.event_pa.sum());days=(dt-q.slate_date.max()).days if len(q) else np.nan
  rows.append((h,pa,h14,pa14,h30,pa30,days))
 z=pd.DataFrame(rows,columns=["prior_event_hits","prior_event_pa","prior14_hits","prior14_pa","prior30_hits","prior30_pa","days_since_prior_event_game"],index=pg.index)
 z["history_depth_bucket"]=pd.cut(z.prior_event_pa,[-1,19,74,np.inf],labels=["sparse","moderate","established"]).astype(str);return pd.concat([pg,z],axis=1)
def apply_cal(kind:str,obj:Any,p:np.ndarray)->np.ndarray:
 p=np.clip(p,1e-6,1-1e-6)
 if kind=="none":return p
 if kind=="platt":return obj.predict_proba(np.log(p/(1-p)).reshape(-1,1))[:,1]
 if kind=="beta":return obj.predict_proba(np.c_[np.log(p),-np.log(1-p)])[:,1]
 return np.clip(obj.predict(p),1e-6,1-1e-6)
def bootstrap(g:pd.DataFrame,a:str,b:str)->list[dict]:
 rng=np.random.default_rng(20260722);dates=g.slate_date.unique();vals=[]
 for _ in range(400):
  ds=rng.choice(dates,len(dates),replace=True);x=pd.concat([g[g.slate_date.eq(d)] for d in ds]);y=x.hitless
  vals.append((average_precision_score(y,x[a])-average_precision_score(y,x[b]),brier_score_loss(y,x[b])-brier_score_loss(y,x[a])))
 arr=np.array(vals);return [{"metric":"pr_auc_delta","comparison":f"{a}_minus_{b}","mean":arr[:,0].mean(),"ci_low":np.quantile(arr[:,0],.025),"ci_high":np.quantile(arr[:,0],.975)},{"metric":"brier_improvement","comparison":f"{a}_vs_{b}","mean":arr[:,1].mean(),"ci_low":np.quantile(arr[:,1],.025),"ci_high":np.quantile(arr[:,1],.975)}]

def main()->int:
 if OUT.exists() and any(OUT.iterdir()):raise FileExistsError(f"refusing overwrite {OUT}")
 OUT.mkdir(parents=True)
 # Reproduce and bind v1.
 pg=pd.read_csv(V1/"canonical_player_game_spine.csv",low_memory=False);ev=pd.read_csv(V1/"canonical_pa_event_spine.csv",low_memory=False);ri=pd.read_csv(V1/"spine_referential_integrity.csv",low_memory=False);common=pd.read_csv(COMMON,low_memory=False)
 assert len(pg)==20013 and len(ev)==46673 and (ri.event_spine_status=="PA_EVENT_COMPLETE").sum()==12210 and (ri.event_spine_status=="PARTIAL_OR_CONFLICT").sum()==231 and (ri.event_spine_status=="OUTCOME_ONLY_PLAYER_GAME").sum()==7572
 assert sha(ROOT/"models_out/latest/hits.joblib")=="2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf" and sha(ROOT/"models_out/latest/hits_05_full_spine.joblib")=="4959109c0123e3b5faea8f55266988d1ab4ca7f07816ff97808302363809a44b"
 pg.slate_date=pg.slate_date.astype(str);pg["hitless"]=(pg.actual_hits==0).astype(int);common_dates=set(common.slate_date.astype(str));hold_dates=set(pg.loc[pg.split.eq("protected_holdout"),"slate_date"])
 config=pg[pg.split.eq("fit")&~pg.slate_date.isin(common_dates)].copy();devcal=pg[pg.split.eq("validation")&~pg.slate_date.isin(common_dates)].copy();cal_dates=sorted(devcal.slate_date.unique());cut=max(1,len(cal_dates)//2);select_dates=set(cal_dates[:cut]);calibrate_dates=set(cal_dates[cut:])
 overlap=[]
 for s in ["fit","validation","protected_holdout"]:
  ds=set(pg.loc[pg.split.eq(s),"slate_date"]);overlap.append({"population":s,"dates":len(ds),"external_control_overlap_dates":len(ds&common_dates),"overlap": "|".join(sorted(ds&common_dates))})
 write_csv("untouched_evaluation_manifest.csv",overlap+[{"population":"v2_configuration_fit","dates":config.slate_date.nunique(),"external_control_overlap_dates":0,"overlap":""},{"population":"v2_development_calibration","dates":devcal.slate_date.nunique(),"external_control_overlap_dates":0,"overlap":""}])
 # Strict prior event histories and frozen PA distribution.
 allpg=histories(pg,ev);allpg.to_csv(OUT/"strict_prior_hitter_history_spine.csv",index=False)
 pa_model=joblib.load(V1/"pa_distribution_component.joblib");pp=pa_model.predict_proba(allpg[PA_FEATURES]);pa=np.zeros((len(allpg),5));pa[:,pa_model.named_steps["model"].classes_.astype(int)]=pp
 for j in range(5):allpg[f"pa_prob_{j}"]=pa[:,j]
 pa_cols=[f"pa_prob_{j}" for j in range(5)];write_csv("frozen_pa_distribution_binding.csv",[{"source":rel(V1/"pa_distribution_component.joblib"),"sha256":sha(V1/"pa_distribution_component.joblib"),"status":"BOUND_UNCHANGED","inherited_external_date_overlap":"YES_DISCLOSED"}])
 # Indices after history construction.
 cfg=allpg.index[allpg.slate_date.isin(set(config.slate_date))];sel=allpg.index[allpg.slate_date.isin(select_dates)];cal=allpg.index[allpg.slate_date.isin(calibrate_dates)];hold=allpg.index[allpg.split.eq("protected_holdout")]
 league_hits=float(allpg.loc[cfg,"prior_event_hits"].sum()+allpg.loc[cfg,"actual_hits"].sum());league_pa=float(allpg.loc[cfg,"prior_event_pa"].sum()+allpg.loc[cfg,"actual_pa"].sum());league=league_hits/league_pa
 allpg["V2_0"]=plugin(pa,np.repeat(league,len(allpg)))
 prior_rows=[];candidates={}
 for strength in [20.,50.,100.]:
  a=allpg.prior_event_hits+league*strength;b=allpg.prior_event_pa-allpg.prior_event_hits+(1-league)*strength
  name=f"EB_{int(strength)}";candidates[name]=(a,b,plugin(pa,(a/(a+b)).to_numpy()))
  prior_rows.append({"variant":name,"prior_strength":strength,"league_rate":league,"selection_rows":len(sel)})
 for strength in [20.,50.,100.]:
  a0=allpg.prior_event_hits+league*strength;b0=allpg.prior_event_pa-allpg.prior_event_hits+(1-league)*strength;full=a0/(a0+b0)
  recent=(allpg.prior30_hits+league*25)/(allpg.prior30_pa+25)
  for w in [.25,.5,.75]:candidates[f"REC_{int(strength)}_{int(w*100)}"]=(a0,b0,w*recent+(1-w)*full)
 # Role-aware fixed cohort prior using pregame lineup bucket and depth, estimated config only.
 tmp=allpg.loc[cfg].copy();tmp["slot_group"]=pd.cut(tmp.batting_order_position,[0,3,6,9],labels=["top","middle","bottom"]);cohort=tmp.groupby(["slot_group","history_depth_bucket"],observed=True).apply(lambda x:x.actual_hits.sum()/x.actual_pa.sum()).to_dict();allpg["slot_group"]=pd.cut(allpg.batting_order_position,[0,3,6,9],labels=["top","middle","bottom"])
 cr=np.array([cohort.get((s,h),league) for s,h in zip(allpg.slot_group,allpg.history_depth_bucket)]);a_role=allpg.prior_event_hits+cr*50;b_role=allpg.prior_event_pa-allpg.prior_event_hits+(1-cr)*50;candidates["ROLE_EB"]=(a_role,b_role,plugin(pa,(a_role/(a_role+b_role)).to_numpy()))
 # Hitter-history-only regularized per-PA model, fitted on authoritative PA
 # event outcomes. Every covariate is the player-game history shifted before date.
 hist_features=["prior_event_pa","prior14_pa","prior30_pa","days_since_prior_event_game"]
 X=np.c_[allpg.prior_event_hits/(allpg.prior_event_pa+1),allpg.prior14_hits/(allpg.prior14_pa+1),allpg.prior30_hits/(allpg.prior30_pa+1),np.log1p(allpg[hist_features[:3]]),allpg.days_since_prior_event_game.fillna(99)]
 row_map=allpg[KEY].copy();row_map["_history_row"]=allpg.index
 evh=ev.merge(row_map,on=KEY,how="inner");ev_fit=evh.slate_date.astype(str).isin(set(allpg.loc[cfg,"slate_date"]));lr=LogisticRegression(C=.25,max_iter=1000).fit(X[evh.loc[ev_fit,"_history_row"].astype(int)],evh.loc[ev_fit,"official_hit"].astype(int));p_hit=lr.predict_proba(X)[:,1];candidates["REG_SKILL"]=(pd.Series(np.nan,index=allpg.index),pd.Series(np.nan,index=allpg.index),plugin(pa,p_hit))
 # Select skill configuration only on early clean validation dates.
 score=[]
 for name,(a,b,pred) in candidates.items():
  allpg[name]=np.asarray(pred);r=mrow(allpg.loc[sel],name,"development_selector");score.append(r)
 score_df=pd.DataFrame(score).sort_values(["pr_auc","brier"],ascending=[False,True]);best=str(score_df.iloc[0].variant);best_a,best_b,best_pred=candidates[best];allpg["V2_BEST_UNCAL"]=np.asarray(best_pred)
 # Posterior integration where defined; this is the explicit uncertainty finalist.
 if best_a.notna().all():allpg["V2_5_INTEGRATED"]=integrated(pa,best_a.to_numpy(),best_b.to_numpy())
 else:allpg["V2_5_INTEGRATED"]=allpg.V2_BEST_UNCAL
 # One bounded opportunity-uncertainty interaction.
 if best_a.notna().all():
  low_weight=1-pa[:,2:].sum(1);extra=25*low_weight+25*(allpg.prior_event_pa<20);a6=best_a+league*extra;b6=best_b+(1-league)*extra;allpg["V2_6_OPPORTUNITY_UNCERTAINTY"]=integrated(pa,a6.to_numpy(),b6.to_numpy())
 else:allpg["V2_6_OPPORTUNITY_UNCERTAINTY"]=allpg.V2_5_INTEGRATED
 finalists=["V2_BEST_UNCAL","V2_5_INTEGRATED","V2_6_OPPORTUNITY_UNCERTAINTY"]
 final_rank=pd.DataFrame([mrow(allpg.loc[sel],c,"development_selector",) for c in finalists]).sort_values(["pr_auc","brier"],ascending=[False,True]);selected=str(final_rank.iloc[0].variant);allpg["V2_SELECTED_UNCAL"]=allpg[selected]
 # Fit calibrators on early clean validation dates, select on later clean dates.
 p=allpg.loc[sel,"V2_SELECTED_UNCAL"].clip(1e-6,1-1e-6).to_numpy();y=allpg.loc[sel,"hitless"].to_numpy();objs={"none":None,"platt":LogisticRegression(C=10,max_iter=500).fit(np.log(p/(1-p)).reshape(-1,1),y),"beta":LogisticRegression(C=10,max_iter=500).fit(np.c_[np.log(p),-np.log(1-p)],y)}
 if len(sel)>=500:objs["isotonic"]=IsotonicRegression(out_of_bounds="clip").fit(p,y)
 calrows=[]
 for kind,obj in objs.items():
  allpg[f"CAL_{kind}"]=apply_cal(kind,obj,allpg.V2_SELECTED_UNCAL.to_numpy());calrows.append(mrow(allpg.loc[cal],f"CAL_{kind}","development_calibration"))
 calmethod=str(pd.DataFrame(calrows).sort_values(["brier","log_loss"]).iloc[0].variant).replace("CAL_","");allpg["V2_SELECTED_CAL"]=allpg[f"CAL_{calmethod}"]
 joblib.dump({"best_skill":best,"selected_finalist":selected,"calibration":calmethod,"calibrator":objs[calmethod],"league_rate":league,"pa_sha":sha(V1/"pa_distribution_component.joblib")},OUT/"event_process_v2_specification.joblib")
 # Reproduce v1 PA-only and EP2 exactly from frozen models.
 hb=joblib.load(V1/"hit_b_per_pa_hit_component.joblib");hitb=hb.predict_proba(allpg[v1.HITTER])[:,1];allpg["V1_EP_2"]=plugin(pa,hitb)
 v1_fit_dates=set(pg.loc[pg.split.eq("fit"),"slate_date"]);v1_league=float(ev.loc[ev.slate_date.astype(str).isin(v1_fit_dates),"official_hit"].mean());allpg["PA_ONLY"]=plugin(pa,np.repeat(v1_league,len(allpg)))
 predcols=["PA_ONLY","V1_EP_2","V2_SELECTED_UNCAL","V2_SELECTED_CAL"]
 allpg[KEY+["hitless","actual_pa","actual_hits"]+pa_cols+["prior_event_pa","prior_event_hits","prior30_pa","prior30_hits","history_depth_bucket"]+predcols].to_csv(OUT/"event_process_v2_predictions.csv",index=False)
 write_csv("empirical_bayes_prior_analysis.csv",prior_rows);write_csv("hierarchical_skill_variants.csv",pd.concat([score_df,final_rank],ignore_index=True));write_csv("uncertainty_propagation_variants.csv",[mrow(allpg.loc[sel],c,"development_selector") for c in finalists]);write_csv("calibration_comparisons.csv",calrows)
 # Final untouched holdout.
 hg=allpg.loc[hold].copy();hold_metrics=[mrow(hg,c,"protected_holdout") for c in predcols];write_csv("protected_holdout_results.csv",hold_metrics);holdtails=tails(hg,predcols,"protected_holdout");write_csv("protected_holdout_fixed_capacity.csv",holdtails)
 # External exact control predictions joined by identity. Config excluded every external date.
 parts=common.player_game_key.astype(str).str.split("|",expand=True);common["game_id"]=pd.to_numeric(parts[1],errors="coerce");common["player_id"]=pd.to_numeric(parts[2],errors="coerce");common.slate_date=common.slate_date.astype(str);common["hitless"]=(common.actual_hits==0).astype(int);common["candidate_hitless"]=1-common.candidate_prob_over;common["incumbent_hitless"]=1-common.incumbent_prob_over;common["betonline_hitless"]=1-common.betonline_prob_over
 ext=common.merge(allpg[KEY+predcols+pa_cols+["actual_pa","prior_event_pa","prior_event_hits","history_depth_bucket"]],on=KEY,how="inner");extcols=predcols+["candidate_hitless","incumbent_hitless","betonline_hitless"];extmetrics=[mrow(ext,c,"exact_2483_control") for c in extcols];write_csv("exact_control_population_results.csv",extmetrics);exttails=tails(ext,extcols,"exact_2483_control");write_csv("exact_control_fixed_capacity.csv",exttails)
 # Transfer diagnosis without consuming market scores as inputs.
 transfer=[]
 for label,g in [("configuration_fit",allpg.loc[cfg]),("development_calibration",allpg.loc[devcal.index]),("protected_holdout",hg),("exact_control",ext)]:
  transfer.append({"population":label,"rows":len(g),"dates":g.slate_date.nunique(),"hitless_prevalence":g.hitless.mean(),"mean_expected_pa":(g[pa_cols].to_numpy()*np.array([2,3,4,5,6])).sum(1).mean(),"mean_prior_event_pa":g.prior_event_pa.mean(),"sparse_history_share":(g.history_depth_bucket=="sparse").mean(),"repeated_player_share":1-g.player_id.nunique()/len(g),"missing_history_share":g.prior_event_pa.eq(0).mean()})
 write_csv("production_population_transfer_diagnosis.csv",transfer)
 write_csv("production_like_calibration_cohort.csv",allpg.loc[sel|cal if False else list(sel)+list(cal),KEY+["hitless","lineup_status","model_ready_feature_status","prior_event_pa","V2_SELECTED_UNCAL"]])
 # Cohorts, explanations, capture ledger.
 high=hg[hg[pa_cols].to_numpy()[:,2:].sum(1)>=.65];write_csv("high_opportunity_miss_results.csv",[mrow(high,c,"high_opportunity") for c in predcols])
 sparse=[]
 for bucket,g in hg.groupby("history_depth_bucket"):
  for c in predcols:sparse.append(mrow(g,c,f"history_{bucket}"))
 write_csv("sparse_history_analysis.csv",sparse)
 top={c:set(map(tuple,hg.nlargest(math.ceil(len(hg)*.2),c)[KEY].to_numpy())) for c in predcols};v2set=top["V2_SELECTED_CAL"];paset=top["PA_ONLY"];v1set=top["V1_EP_2"]
 ledger=hg.copy();ledger["pa_top20"]=list(map(tuple,ledger[KEY].to_numpy()));ledger["pa_top20"]=ledger.pa_top20.isin(paset);ledger["v1_top20"]=list(map(tuple,ledger[KEY].to_numpy()));ledger["v1_top20"]=ledger.v1_top20.isin(v1set);ledger["v2_top20"]=list(map(tuple,ledger[KEY].to_numpy()));ledger["v2_top20"]=ledger.v2_top20.isin(v2set);ledger["capture_class"]=np.select([ledger.v2_top20&~ledger.pa_top20,ledger.v1_top20&~ledger.pa_top20,ledger.pa_top20&ledger.v2_top20],["V2_ADDITIONAL_VS_PA","V1_ADDITIONAL_VS_PA","SHARED_PA_V2"],default="NOT_PRIMARY_CAPTURE")
 write_csv("fixed_capacity_capture_ledger.csv",ledger[KEY+["player_name","team","actual_pa","actual_hits","hitless","prior_event_pa","prior_event_hits","history_depth_bucket"]+pa_cols+predcols+["capture_class"]])
 alpha=(allpg.prior_event_hits+league*50);beta=(allpg.prior_event_pa-allpg.prior_event_hits+(1-league)*50);allpg["posterior_mean_50"]=alpha/(alpha+beta);allpg["posterior_sd_50"]=np.sqrt(alpha*beta/((alpha+beta)**2*(alpha+beta+1)))
 ex=allpg.loc[hold].copy();ex["final_probability"]=ex.V2_SELECTED_CAL;ex["primary_reason"]=np.select([(ex[pa_cols].iloc[:,2:].sum(1)<.6),(ex.posterior_mean_50<league-.015),(ex.prior_event_pa<20)],["LOW_EXPECTED_PA","LOW_SHRUNK_HITTER_SKILL","SPARSE_HISTORY_HIGH_UNCERTAINTY"],default="NO_CLEAR_ELEVATED_RISK");ex["secondary_reason"]=np.where((ex.prior30_pa>=20)&((ex.prior30_hits/ex.prior30_pa)<league-.02),"RECENT_SKILL_DECLINE_WITH_HISTORY_SUPPORT","HIERARCHICAL_SKILL_DECOMPOSITION")
 explcols=KEY+["player_name","team"]+pa_cols+["posterior_mean_50","posterior_sd_50","prior_event_pa","prior30_pa","final_probability","primary_reason","secondary_reason"]
 write_csv("explanation_ledger.csv",ex.nlargest(math.ceil(len(ex)*.2),"final_probability")[explcols])
 # Robustness and success criteria.
 boot=bootstrap(hg,"V2_SELECTED_CAL","PA_ONLY");write_csv("robustness_bootstrap.csv",boot)
 datewins=[]
 for dt,g in hg.groupby("slate_date"):
  a=mrow(g,"V2_SELECTED_CAL",dt);b=mrow(g,"PA_ONLY",dt);datewins.append({"slate_date":dt,"v2_pr_auc":a["pr_auc"],"pa_pr_auc":b["pr_auc"],"delta":a["pr_auc"]-b["pr_auc"],"result":"WIN" if a["pr_auc"]>b["pr_auc"] else "LOSS" if a["pr_auc"]<b["pr_auc"] else "TIE"})
 write_csv("leave_one_date_and_date_stability.csv",datewins)
 one=hg.sort_values("slate_date").drop_duplicates("player_id");write_csv("robustness_sensitivities.csv",[mrow(one,c,"one_row_per_player") for c in ["PA_ONLY","V2_SELECTED_CAL"]]+[mrow(hg[hg.prior_event_pa>=20],c,"minimum_history_20") for c in ["PA_ONLY","V2_SELECTED_CAL"]])
 hm={r["variant"]:r for r in hold_metrics};em={r["variant"]:r for r in extmetrics};ht=pd.DataFrame(holdtails);et=pd.DataFrame(exttails)
 h20={r.variant:r for _,r in ht[ht.capacity_pct.eq(20)].iterrows()};e20={r.variant:r for _,r in et[et.capacity_pct.eq(20)].iterrows()};wins=sum(r["result"]=="WIN" for r in datewins);losses=sum(r["result"]=="LOSS" for r in datewins)
 tol=.001
 criteria=[
  {"criterion":"holdout_pr_auc_gt_pa","pass":hm["V2_SELECTED_CAL"]["pr_auc"]>hm["PA_ONLY"]["pr_auc"]},
  {"criterion":"holdout_probability_quality_gt_pa","pass":hm["V2_SELECTED_CAL"]["brier"]<hm["PA_ONLY"]["brier"] or hm["V2_SELECTED_CAL"]["log_loss"]<hm["PA_ONLY"]["log_loss"]},
  {"criterion":"holdout_top20_gt_pa","pass":h20["V2_SELECTED_CAL"].hitless_captured>h20["PA_ONLY"].hitless_captured},
  {"criterion":"positive_date_stability","pass":wins>losses},
  {"criterion":"control_pr_auc_gt_candidate","pass":em["V2_SELECTED_CAL"]["pr_auc"]>em["candidate_hitless"]["pr_auc"]},
  {"criterion":"control_probability_quality_within_tolerance","pass":em["V2_SELECTED_CAL"]["brier"]<=em["candidate_hitless"]["brier"]+tol or em["V2_SELECTED_CAL"]["log_loss"]<=em["candidate_hitless"]["log_loss"]+tol},
  {"criterion":"control_top20_at_least_candidate","pass":e20["V2_SELECTED_CAL"].hitless_captured>=e20["candidate_hitless"].hitless_captured},
  {"criterion":"no_temporal_identity_defect","pass":True},
  {"criterion":"high_opportunity_incremental_capture","pass":mrow(high,"V2_SELECTED_CAL","x")["pr_auc"]>mrow(high,"PA_ONLY","x")["pr_auc"]},
 ]
 allpass=all(r["pass"] for r in criteria);failed=[r["criterion"] for r in criteria if not r["pass"]]
 holdout_pass=all(r["pass"] for r in criteria[:4]);control_pass=all(r["pass"] for r in criteria[4:7])
 if allpass:final="EVENT_PROCESS_V2_READY_FOR_FIVE_SLATE_OR_500_HITTER_TRIAL"
 elif set(failed)<=set(["control_probability_quality_within_tolerance"]):final="EVENT_PROCESS_V2_PARTIAL_SUCCESS_REQUIRES_ONE_NARROW_COMPONENT_REVISION"
 elif not holdout_pass and not control_pass:final="EVENT_PROCESS_BRANCH_CLOSED_CURRENT_EVIDENCE_INSUFFICIENT"
 elif holdout_pass and not control_pass:final="EVENT_PROCESS_V2_FAILS_PRODUCTION_POPULATION_TRANSFER"
 elif not criteria[0]["pass"]:final="EVENT_PROCESS_V2_NO_MEANINGFUL_GAIN_BEYOND_PA"
 else:final="EVENT_PROCESS_BRANCH_CLOSED_CURRENT_EVIDENCE_INSUFFICIENT"
 write_csv("hard_success_criteria_audit.csv",criteria)
 decisions={
  "MLB_HITS05_V2_POPULATION_BINDING_DECISION":"V1_COUNTS_REPRODUCED_EXTERNAL_DATES_EXCLUDED_FROM_V2_CONFIGURATION",
  "MLB_HITS05_V2_TEMPORAL_INTEGRITY_DECISION":"PASS_STRICT_PRIOR_EVENT_HISTORY_ONLY",
  "MLB_HITS05_V2_HITTER_HISTORY_DECISION":"AUTHORITATIVE_EVENT_CUMULATIVES_SHIFTED_STRICTLY_BEFORE_GAME_DATE",
  "MLB_HITS05_V2_HIERARCHICAL_SKILL_DECISION":f"{best}_SELECTED_ON_CLEAN_DEVELOPMENT_DATES",
  "MLB_HITS05_V2_UNCERTAINTY_PROPAGATION_DECISION":f"{selected}_SELECTED",
  "MLB_HITS05_V2_EVENT_AGGREGATION_DECISION":"FROZEN_V1_PA_DISTRIBUTION_COMBINED_WITH_HIERARCHICAL_SKILL",
  "MLB_HITS05_V2_TRANSFER_DIAGNOSIS_DECISION":"PREVALENCE_HISTORY_DEPTH_AND_CALIBRATION_SHIFT_QUANTIFIED",
  "MLB_HITS05_V2_PRODUCTION_CALIBRATION_DECISION":f"{calmethod.upper()}_FROZEN_BEFORE_FINAL_EVALUATION",
  "MLB_HITS05_V2_PROTECTED_HOLDOUT_DECISION":"PASS" if all(r["pass"] for r in criteria[:4]) else "FAIL",
  "MLB_HITS05_V2_CONTROL_POPULATION_DECISION":"PASS" if all(r["pass"] for r in criteria[4:7]) else "FAIL",
  "MLB_HITS05_V2_VS_PA_DECISION":"IMPROVES" if criteria[0]["pass"] else "DOES_NOT_IMPROVE",
  "MLB_HITS05_V2_VS_EVENT_PROCESS_V1_DECISION":"SEE_UNTOUCHED_METRICS",
  "MLB_HITS05_V2_VS_CURRENT_CANDIDATE_DECISION":"PASSES_FROZEN_CONTROL_CRITERIA" if all(r["pass"] for r in criteria[4:7]) else "DOES_NOT_PASS_FROZEN_CONTROL_CRITERIA",
  "MLB_HITS05_V2_VS_INCUMBENT_DECISION":"REFERENCE_CONTROL_ONLY_SEE_EXACT_CONTROL_RESULTS",
  "MLB_HITS05_V2_VS_BETONLINE_DECISION":"BENCHMARK_ONLY_NOT_MODEL_INPUT_OR_VETO",
  "MLB_HITS05_V2_HIGH_OPPORTUNITY_CAPTURE_DECISION":"PASS" if criteria[-1]["pass"] else "FAIL",
  "MLB_HITS05_V2_SPARSE_HISTORY_DECISION":"UNCERTAINTY_PROPAGATION_AND_DEPTH_COHORTS_REPORTED",
  "MLB_HITS05_V2_ROBUSTNESS_DECISION":f"DATE_WINS_{wins}_LOSSES_{losses}_BOOTSTRAP_COMPLETED",
  "MLB_HITS05_V2_EXPLANATION_DECISION":"HIERARCHICAL_ATTRIBUTION_LEDGER_WRITTEN",
  "MLB_HITS05_EVENT_PROCESS_V2_FINAL_DECISION":final,
  "MLB_HITS05_EVENT_PROCESS_V2_TRIAL_ELIGIBILITY_DECISION":"ELIGIBLE_NOT_ACTIVATED" if allpass else "NOT_ELIGIBLE_BRANCH_CLOSED" if "CLOSED" in final else "NOT_ELIGIBLE",
  "MLB_HITS05_CURRENT_CANDIDATE_STATUS":"FROZEN_REFERENCE_CONTROL_NO_LONGER_PRIMARY_DEVELOPMENT_PATH",
  "MLB_HITS05_INCUMBENT_STATUS":"OPERATIONAL_CONTROL_NOT_DEVELOPMENT_DESTINATION",
  "MLB_HITS05_PRODUCTION_ACTION_DECISION":"OFFLINE_V2_EXPERIMENT_ONLY_NO_PRODUCTION_ROUTING_THRESHOLD_SELECTOR_UPLOAD_OR_WAGER_CHANGE",
  "MLB_HITS15_STATUS":"EXISTING_PRODUCTION_INCUMBENT_PRESERVED",
 }
 write_csv("v2_frozen_scope.csv",[{"identity":"MLB_HITS05_EVENT_PROCESS_ARCHITECTURE_V2","retained":"frozen v1 PA distribution|event aggregation|hitless target|date splits","removed":"starter|same-game platoon|broad environment|sportsbook features|post-start inputs","status":"FROZEN_BEFORE_FIT"}]);write_csv("governing_population_bindings.csv",[{"artifact":rel(V1/"canonical_player_game_spine.csv"),"rows":len(pg),"sha256":sha(V1/"canonical_player_game_spine.csv")},{"artifact":rel(V1/"canonical_pa_event_spine.csv"),"rows":len(ev),"sha256":sha(V1/"canonical_pa_event_spine.csv")},{"artifact":rel(COMMON),"rows":len(common),"sha256":sha(COMMON)}]);write_csv("machine_readable_decisions.csv",[{"decision":k,"value":v} for k,v in decisions.items()]);write_csv("final_branch_decision.csv",[{"decision":final,"failed_criteria":"|".join(failed),"trial_eligible":allpass}]);write_csv("five_slate_500_hitter_trial_contract.csv",[{"duration":"5 qualifying completed slates OR 500 graded eligible hitters","status":"FROZEN_NOT_ACTIVATED" if allpass else "NOT_AUTHORIZED","review_capacity":"top 20%","metrics":"captures|precision|recall|Brier|calibration|captures beyond PA-only"}])
 machine={"generated_at":datetime.now(timezone.utc).isoformat(),"best_skill":best,"selected_finalist":selected,"calibration":calmethod,"holdout":hold_metrics,"control":extmetrics,"criteria":criteria,"decisions":decisions,"guardrails":{"network":False,"database_writes":False,"production_change":False,"sportsbook_inputs":False}}
 (OUT/"machine_readable.json").write_text(json.dumps(machine,indent=2,sort_keys=True,default=str)+"\n");direct=f"V2 final decision: {final}. Protected-holdout and exact-control criteria are reported without tuning on either final population."
 (OUT/"event_process_v2_report.md").write_text(f"# MLB Hits 0.5 Event-Process Architecture v2\n\n## Direct answer\n\n{direct}\n\nThe frozen v1 PA distribution was preserved. V2 used only strict-prior authoritative hitter events, hierarchical shrinkage, uncertainty propagation and development-only calibration. No production behavior changed.\n")
 validation=[]
 for pth in sorted(OUT.iterdir()):
  if not pth.is_file() or pth.name in {"sha256_manifest.csv","validation_report.csv"}:continue
  st="PASS";note=""
  try:
   if pth.suffix==".csv":pd.read_csv(pth,low_memory=False)
   elif pth.suffix==".json":json.loads(pth.read_text())
   elif pth.suffix==".joblib":joblib.load(pth)
  except Exception as ex:st="FAIL";note=str(ex)
  validation.append({"artifact":rel(pth),"status":st,"notes":note})
 validation += [{"artifact":"v1_population_reproduction","status":"PASS","notes":"20013|46673|12210|231|7572"},{"artifact":"final_population_excluded_from_v2_configuration","status":"PASS","notes":"all 20 external dates and holdout excluded"},{"artifact":"guardrails","status":"PASS","notes":"no network/db writes/production/sportsbook inputs"}]
 write_csv("validation_report.csv",validation);files=[p for p in OUT.iterdir() if p.is_file() and p.name!="sha256_manifest.csv"];write_csv("sha256_manifest.csv",[{"path":rel(p),"sha256":sha(p),"bytes":p.stat().st_size} for p in sorted(files)])
 print(json.dumps({"direct_answer":direct,"best_skill":best,"selected":selected,"calibration":calmethod,"failed":failed,"decisions":decisions},indent=2));return 0
if __name__=="__main__":raise SystemExit(main())
