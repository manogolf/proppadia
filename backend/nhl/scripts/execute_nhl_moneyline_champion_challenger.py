#!/usr/bin/env python3
"""Execute once the frozen NHL moneyline schedule-context challenger."""
from __future__ import annotations
import argparse, hashlib, json, subprocess
from pathlib import Path
import numpy as np
import pandas as pd
from backend.nhl.analysis_package_guard import require_create_only
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score,brier_score_loss,log_loss,roc_auc_score
from sklearn.preprocessing import StandardScaler

DATE="2026-07-13"; SEED=20260713; CHAMP="NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1"; CHALL="NHL_MONEYLINE_SCHEDULE_LOAD_CONTEXT_LOGIT_CHALLENGER_V1"; CHAMP_HASH="83beb11588f7e7e31919f23be2dea51ff49863954fc9be750509b30a0eff2cda"; TOL=1e-12
PARENTS={"certification":("nhl_moneyline_frozen_baseline_certification","8bb36073fee4f055f399c651f942b8de6eb1bb3b75b96b6112dd9d4af4224cf5"),"specification":("nhl_moneyline_champion_challenger_specification","ed07fa1ac76eb229d514fa24af1d1d22706c92eb539d4422f73892b76f6922b4")}
CONTROL=["diff_std_goal_diff_pg","diff_r10_goal_diff_pg","diff_std_shot_diff_pg","diff_days_rest","home_back_to_back","away_back_to_back"]; NEW=["diff_games_prior_5d","home_consecutive_road_games_prior","away_consecutive_road_games_prior"]; FEATURES=CONTROL+NEW
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def csv(d,p): d.to_csv(p,index=False,lineterminator="\n",float_format="%.15g")
def js(o,p): Path(p).write_text(json.dumps(o,indent=2,sort_keys=True,allow_nan=False)+"\n")
def metric(y,p):
 y=np.asarray(y,int); p=np.asarray(p,float)
 return {"rows":len(y),"home_wins":int(y.sum()),"accuracy":accuracy_score(y,p>=.5),"brier_score":brier_score_loss(y,p),"log_loss":log_loss(y,p,labels=[0,1]),"roc_auc":roc_auc_score(y,p) if len(np.unique(y))>1 and len(np.unique(p))>1 else np.nan,"mean_probability":p.mean(),"observed_home_win_rate":y.mean()}
def ece(y,p):
 y=np.asarray(y); p=np.asarray(p); b=np.minimum((p*10).astype(int),9)
 return sum((b==i).sum()*abs(p[b==i].mean()-y[b==i].mean()) for i in range(10) if (b==i).any())/len(y)
def paired(y,c,h):
 cm=metric(y,c); hm=metric(y,h)
 return {"rows":len(y),"champion_brier":cm['brier_score'],"challenger_brier":hm['brier_score'],"brier_improvement":cm['brier_score']-hm['brier_score'],"champion_log_loss":cm['log_loss'],"challenger_log_loss":hm['log_loss'],"log_loss_improvement":cm['log_loss']-hm['log_loss'],"champion_roc_auc":cm['roc_auc'],"challenger_roc_auc":hm['roc_auc'],"roc_auc_change":hm['roc_auc']-cm['roc_auc'],"champion_accuracy":cm['accuracy'],"challenger_accuracy":hm['accuracy'],"accuracy_change":hm['accuracy']-cm['accuracy'],"champion_ece":ece(y,c),"challenger_ece":ece(y,h),"ece_degradation":ece(y,h)-ece(y,c)}
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[3]); ap.add_argument('--output-dir',type=Path); a=ap.parse_args(); root=a.repo_root.resolve(); base=root/'artifacts/analysis/model_development'; out=(a.output_dir or base/f'nhl_moneyline_champion_challenger_execution/{DATE}').resolve()
 pp={k:base/s/DATE for k,(s,h) in PARENTS.items()}; before={str(f):sha(f) for p in pp.values() for f in p.iterdir() if f.is_file()}
 for k,p in pp.items(): assert sha(p/'SHA256SUMS')==PARENTS[k][1]; subprocess.run(['shasum','-a','256','-c','SHA256SUMS'],cwd=p,check=True,capture_output=True)
 require_create_only(out);out.mkdir(parents=True)
 specp=pp['specification']; hyp=json.loads((specp/f'nhl_moneyline_selected_challenger_hypothesis_{DATE}.json').read_text()); fm=pd.read_csv(specp/f'nhl_moneyline_challenger_feature_manifest_{DATE}.csv'); popc=json.loads((specp/f'nhl_moneyline_challenger_population_contract_{DATE}.json').read_text()); temporal=json.loads((specp/f'nhl_moneyline_challenger_temporal_protocol_{DATE}.json').read_text()); success=json.loads((specp/f'nhl_moneyline_challenger_success_criteria_{DATE}.json').read_text())
 assert hyp['challenger_name']==CHALL and fm.feature_name.tolist()==FEATURES and fm.timing_status.eq('CERTIFIED_STRICT_PRIOR').all() and popc['rows']==2798 and [temporal[x]['rows'] for x in ['fit','validation','holdout']]==[701,699,1398]
 baseline=base/f'nhl_moneyline_simple_baseline_process_validation/{DATE}'; predp=baseline/f'nhl_moneyline_simple_baseline_control_predictions_{DATE}.csv'; assert sha(predp)==CHAMP_HASH
 champion=pd.read_csv(predp); part=pd.read_csv(baseline/f'nhl_moneyline_simple_baseline_population_partition_{DATE}.csv'); ma=pd.read_csv(baseline/f'nhl_moneyline_simple_baseline_feature_matrix_audit_{DATE}.csv'); keys=['canonical_season','game_id']; assert len(champion)==2798 and champion[keys].duplicated().sum()==0
 spinep=base/f'nhl_moneyline_team_goalie_feature_spine/{DATE}'; schedp=base/f'nhl_season_2024_utah_game_date_remediation/{DATE}'; subprocess.run(['shasum','-a','256','-c','SHA256SUMS'],cwd=spinep,check=True,capture_output=True); subprocess.run(['shasum','-a','256','-c','SHA256SUMS'],cwd=schedp,check=True,capture_output=True)
 spine=pd.read_csv(spinep/f'nhl_moneyline_team_feature_spine_{DATE}.csv',usecols=keys+['home_games_prior_5d','away_games_prior_5d','home_consecutive_road_games_prior','away_consecutive_road_games_prior']); sched=pd.read_csv(schedp/f'nhl_season_2024_schedule_rebuild_{DATE}.csv'); s=sched.set_index(keys); mask=spine.canonical_season.eq(2024); idx=pd.MultiIndex.from_frame(spine.loc[mask,keys])
 for x in ['home_games_prior_5d','away_games_prior_5d','home_consecutive_road_games_prior','away_consecutive_road_games_prior']: spine.loc[mask,x]=s.loc[idx,x].to_numpy()
 spine['diff_games_prior_5d']=spine.home_games_prior_5d-spine.away_games_prior_5d
 raw=ma[keys+[f'raw__{x}' for x in CONTROL]].copy(); raw.columns=keys+CONTROL; raw=raw.merge(spine[keys+NEW],on=keys,validate='one_to_one'); assert len(raw)==2798 and raw[NEW].notna().all().all()
 d=champion.merge(raw,on=keys,validate='one_to_one').merge(part[keys+['missingness_status']],on=keys,validate='one_to_one',suffixes=('','_partition')); assert d.groupby('split').size().to_dict()=={'fit':701,'validation':699,'holdout':1398}; assert d[FEATURES].columns.tolist()==FEATURES
 # Exactly one challenger fit. Validation and holdout targets are not used before this call.
 fit=d.split.eq('fit'); imp=SimpleImputer(strategy='median'); scale=StandardScaler(); xfit=imp.fit_transform(d.loc[fit,FEATURES]); xsfit=scale.fit_transform(xfit); model=LogisticRegression(penalty='l2',C=1.0,solver='liblinear',random_state=SEED,max_iter=1000,tol=1e-4,fit_intercept=True); model.fit(xsfit,d.loc[fit,'home_win_target'].astype(int)); ximp=imp.transform(d[FEATURES]); xs=scale.transform(ximp); hp=model.predict_proba(xs)[:,1]
 assert np.isfinite(hp).all() and ((hp>=0)&(hp<=1)).all()
 audit=d[keys+['game_date','split','missingness_status']].copy(); audit['raw_missing_count']=d[FEATURES].isna().sum(axis=1)
 for i,x in enumerate(FEATURES): audit[f'raw__{x}']=d[x]; audit[f'imputed__{x}']=ximp[:,i]; audit[f'scaled__{x}']=xs[:,i]; audit[f'was_imputed__{x}']=d[x].isna()
 csv(audit,out/f'nhl_moneyline_challenger_feature_matrix_audit_{DATE}.csv')
 params=[]
 for i,x in enumerate(FEATURES): params.append([i+1,x,model.coef_[0,i],imp.statistics_[i],scale.mean_[i],scale.scale_[i],model.coef_[0,i]>0])
 params.append([0,'__INTERCEPT__',model.intercept_[0],np.nan,np.nan,np.nan,model.intercept_[0]>0]); pdf=pd.DataFrame(params,columns=['feature_order','parameter','coefficient','fit_imputation_median','fit_scaler_mean','fit_scaler_scale','positive_sign']); csv(pdf,out/f'nhl_moneyline_challenger_fitted_parameters_{DATE}.csv')
 y=d.home_win_target.to_numpy(int); cp=d.home_win_probability.to_numpy(float); eps=1e-15
 pr=d[keys+['game_date','home_team','away_team','split','home_win_target','missingness_status']].copy(); pr['champion_home_win_probability']=cp; pr['challenger_home_win_probability']=hp; pr['champion_correctness']=((cp>=.5)==y); pr['challenger_correctness']=((hp>=.5)==y); pr['champion_brier_contribution']=(cp-y)**2; pr['challenger_brier_contribution']=(hp-y)**2; pr['champion_log_loss_contribution']=-(y*np.log(np.clip(cp,eps,1-eps))+(1-y)*np.log(np.clip(1-cp,eps,1-eps))); pr['challenger_log_loss_contribution']=-(y*np.log(np.clip(hp,eps,1-eps))+(1-y)*np.log(np.clip(1-hp,eps,1-eps))); pr['probability_difference_challenger_minus_champion']=hp-cp; csv(pr,out/f'nhl_moneyline_champion_challenger_predictions_{DATE}.csv')
 metrics=[]; diffs=[]
 for split,m in [('fit',d.split.eq('fit')),('validation',d.split.eq('validation')),('holdout',d.split.eq('holdout')),('combined_out_of_time',d.split.isin(['validation','holdout']))]:
  for system,p in [('champion',cp),('challenger',hp)]: metrics.append({'scope':split,'system':system,**metric(y[m],p[m]),'ece':ece(y[m],p[m])})
  diffs.append({'scope':split,**paired(y[m],cp[m],hp[m])})
 metrics=pd.DataFrame(metrics); diffs=pd.DataFrame(diffs); csv(metrics,out/f'nhl_moneyline_champion_challenger_metrics_{DATE}.csv'); csv(diffs,out/f'nhl_moneyline_champion_challenger_metric_differences_{DATE}.csv')
 cal=[]
 for scope,m in [('validation',d.split.eq('validation')),('holdout',d.split.eq('holdout')),('combined_out_of_time',d.split.isin(['validation','holdout']))]:
  for system,p in [('champion',cp),('challenger',hp)]:
   b=np.minimum((p[m]*10).astype(int),9); yy=y[m]; ppv=p[m]; rates=[]
   for i in range(10):
    z=b==i; cal.append([scope,system,'bucket',i,int(z.sum()),ppv[z].mean() if z.any() else np.nan,yy[z].mean() if z.any() else np.nan,abs(ppv[z].mean()-yy[z].mean()) if z.any() else np.nan,np.nan,np.nan]);
    if z.any(): rates.append(yy[z].mean())
   cal.append([scope,system,'summary','ALL',len(yy),ppv.mean(),yy.mean(),abs(ppv.mean()-yy.mean()),ece(yy,ppv),int((np.diff(rates)<0).sum())])
 cal=pd.DataFrame(cal,columns=['scope','system','record_type','bucket','rows','mean_probability','observed_home_win_rate','absolute_calibration_gap','ece','monotonicity_reversals']); csv(cal,out/f'nhl_moneyline_champion_challenger_calibration_{DATE}.csv')
 # Paired calendar-date cluster bootstrap over combined out-of-time rows.
 oot=d.split.isin(['validation','holdout']); od=d.loc[oot].copy(); od['cp']=cp[oot]; od['hp']=hp[oot]; od['y']=y[oot]; dates=sorted(od.game_date.unique()); bydate={dt:od.index[od.game_date.eq(dt)].to_numpy() for dt in dates}; pos={ix:i for i,ix in enumerate(od.index)}; rng=np.random.default_rng(SEED); boots=[]
 oy=od.y.to_numpy(); oc=od.cp.to_numpy(); oh=od.hp.to_numpy()
 for n in range(5000):
  draw=rng.choice(dates,size=len(dates),replace=True); inds=np.concatenate([bydate[x] for x in draw]); loc=np.array([pos[x] for x in inds]); yy=oy[loc]; cc=oc[loc]; hh=oh[loc]; cm=metric(yy,cc); hm=metric(yy,hh); boots.append([n+1,len(yy),cm['brier_score']-hm['brier_score'],cm['log_loss']-hm['log_loss'],hm['roc_auc']-cm['roc_auc']])
 boots=pd.DataFrame(boots,columns=['resample','sampled_rows','brier_improvement','log_loss_improvement','roc_auc_change']); csv(boots,out/f'nhl_moneyline_champion_challenger_bootstrap_{DATE}.csv')
 # Frozen stability slices.
 d['month']=pd.to_datetime(d.game_date).dt.strftime('%Y-%m'); ootmask=d.split.isin(['validation','holdout']); month=[]
 for mo in sorted(d.loc[ootmask,'month'].unique()):
  direct=ootmask&d.month.eq(mo); excl=ootmask&~d.month.eq(mo); month.append({'record_type':'direct_month','month':mo,**paired(y[direct],cp[direct],hp[direct])}); month.append({'record_type':'leave_one_month_out','month':mo,**paired(y[excl],cp[excl],hp[excl])})
 month=pd.DataFrame(month); csv(month,out/f'nhl_moneyline_champion_challenger_month_stability_{DATE}.csv')
 teams=[]
 for appearance,col in [('home','home_team'),('away','away_team')]:
  for team in sorted(d.loc[ootmask,col].unique()):
   m=ootmask&d[col].eq(team); teams.append({'record_type':'team_appearance','team':team,'appearance':appearance,**paired(y[m],cp[m],hp[m])})
 for team in sorted(set(d.loc[ootmask,'home_team'])|set(d.loc[ootmask,'away_team'])):
  m=ootmask&~(d.home_team.eq(team)|d.away_team.eq(team)); teams.append({'record_type':'leave_one_team_out','team':team,'appearance':'both',**paired(y[m],cp[m],hp[m])})
 teams=pd.DataFrame(teams); csv(teams,out/f'nhl_moneyline_champion_challenger_team_stability_{DATE}.csv')
 margin=[]; bands=pd.Series(pd.cut(abs(cp-.5),[0,.025,.05,.10,.15,np.inf],labels=['lt_0.025','0.025_to_0.05','0.05_to_0.10','0.10_to_0.15','gte_0.15'],right=False),index=d.index)
 for b in bands.cat.categories:
  m=ootmask&bands.eq(b); margin.append({'margin_band':b,'record_type':'direct_band',**paired(y[m],cp[m],hp[m])})
 margin=pd.DataFrame(margin); best=margin.loc[margin.brier_improvement.idxmax(),'margin_band']; m=ootmask&~bands.eq(best); margin=pd.concat([margin,pd.DataFrame([{'margin_band':best,'record_type':'exclude_best_band',**paired(y[m],cp[m],hp[m])}])],ignore_index=True); csv(margin,out/f'nhl_moneyline_champion_challenger_margin_stability_{DATE}.csv')
 missing=[]
 for state in ['FULLY_OBSERVED','MINIMUM_HISTORY_LIMITED_IMPUTED']:
  m=ootmask&d.missingness_status.eq(state); missing.append({'missingness_state':state,**paired(y[m],cp[m],hp[m])})
 missing=pd.DataFrame(missing); csv(missing,out/f'nhl_moneyline_champion_challenger_missingness_stability_{DATE}.csv')
 novelty=[]; coefmap=pdf.set_index('parameter').coefficient
 for x in NEW:
  i=FEATURES.index(x); contrib=xs[:,i]*coefmap[x]; corrs={f:pd.Series(d[x]).corr(pd.Series(d[f])) for f in CONTROL}; mx=max(corrs,key=lambda k:abs(corrs[k]) if pd.notna(corrs[k]) else -1); expected={'diff_games_prior_5d':'NEGATIVE','home_consecutive_road_games_prior':'NEGATIVE','away_consecutive_road_games_prior':'POSITIVE'}[x]; sign='POSITIVE' if coefmap[x]>0 else 'NEGATIVE'
  novelty.append({'feature_name':x,'coefficient':coefmap[x],'coefficient_sign':sign,'expected_hypothesis_sign':expected,'sign_aligns_with_hypothesis':sign==expected,'standardized_magnitude':abs(coefmap[x]),'contribution_variance':np.var(contrib),'contribution_q01':np.quantile(contrib,.01),'contribution_q99':np.quantile(contrib,.99),'maximum_absolute_correlation_with_control':abs(corrs[mx]),'most_correlated_control_feature':mx,'correlation_with_most_correlated_control':corrs[mx]})
 novelty=pd.DataFrame(novelty); csv(novelty,out/f'nhl_moneyline_challenger_information_novelty_audit_{DATE}.csv')
 # Frozen gate ledger; no criteria are added after this point.
 get=lambda scope:diffs.set_index('scope').loc[scope]; v=get('validation'); h=get('holdout'); o=get('combined_out_of_time'); bm=(boots.brier_improvement>0).mean(); lm=(boots.log_loss_improvement>0).mean(); mex=month[month.record_type.eq('leave_one_month_out')]; tex=teams[teams.record_type.eq('leave_one_team_out')]; direct=margin[margin.record_type.eq('direct_band')]; nobest=margin[margin.record_type.eq('exclude_best_band')].iloc[0]; full=missing[missing.missingness_state.eq('FULLY_OBSERVED')].iloc[0]
 gates=[]
 def gate(name,required,observed,passed,notes): gates.append([name,required,observed,bool(passed),'PASS' if passed else 'FAIL',notes])
 gate('validation_brier','> 0',v.brier_improvement,v.brier_improvement>0,'champion minus challenger'); gate('validation_log_loss','> 0',v.log_loss_improvement,v.log_loss_improvement>0,'champion minus challenger'); gate('validation_ece_degradation','<= 0.010',v.ece_degradation,v.ece_degradation<=.010,'challenger minus champion ECE')
 gate('holdout_brier','> 0',h.brier_improvement,h.brier_improvement>0,'champion minus challenger'); gate('holdout_log_loss','> 0',h.log_loss_improvement,h.log_loss_improvement>0,'champion minus challenger'); gate('holdout_roc_auc_change','>= -0.005',h.roc_auc_change,h.roc_auc_change>=-.005,'challenger minus champion'); gate('holdout_ece_degradation','<= 0.010',h.ece_degradation,h.ece_degradation<=.010,'challenger minus champion ECE')
 gate('combined_oot_brier','> 0',o.brier_improvement,o.brier_improvement>0,'champion minus challenger'); gate('combined_oot_log_loss','> 0',o.log_loss_improvement,o.log_loss_improvement>0,'champion minus challenger')
 gate('bootstrap_brier_positive_fraction','>= 0.80',bm,bm>=.8,'5000 paired date-cluster resamples'); gate('bootstrap_log_loss_positive_fraction','>= 0.80',lm,lm>=.8,'5000 paired date-cluster resamples')
 mf=((mex.brier_improvement>0)&(mex.log_loss_improvement>0)).mean(); mw=~((mex.brier_improvement<-.002)&(mex.log_loss_improvement<-.002)).any(); gate('month_exclusion_fraction','>= 0.75',mf,mf>=.75,'both improvements positive'); gate('month_exclusion_worst_joint','no exclusion both < -0.002',int(mw),mw,'1 means condition satisfied')
 tf=((tex.brier_improvement>0)&(tex.log_loss_improvement>0)).mean(); tw=~((tex.brier_improvement<-.002)&(tex.log_loss_improvement<-.002)).any(); gate('team_exclusion_fraction','>= 0.75',tf,tf>=.75,'both improvements positive'); gate('team_exclusion_worst_joint','no exclusion both < -0.002',int(tw),tw,'1 means condition satisfied')
 ng=(direct.brier_improvement>=0).sum(); gate('margin_nonnegative_bands','>= 3 of 5',ng,ng>=3,'fixed champion margin bands'); gate('margin_excluding_best_band','> 0',nobest.brier_improvement,nobest.brier_improvement>0,f'excluded {best}')
 gate('fully_observed_brier','> 0',full.brier_improvement,full.brier_improvement>0,'combined out-of-time fully observed'); gate('fully_observed_log_loss','> 0',full.log_loss_improvement,full.log_loss_improvement>0,'combined out-of-time fully observed')
 gates=pd.DataFrame(gates,columns=['gate_name','required_threshold','observed_value','passed','status','notes']); csv(gates,out/f'nhl_moneyline_champion_challenger_gate_ledger_{DATE}.csv'); allpass=bool(gates.passed.all())
 if allpass: classification='CHALLENGER_PASSED_ALL_FROZEN_GATES'; nexttask='PROMOTION_GRADE_HISTORICAL_EVALUATION_SPECIFICATION'
 elif o.brier_improvement>0 or o.log_loss_improvement>0: classification='CHALLENGER_SHOWED_PARTIAL_SIGNAL_BUT_FAILED_FROZEN_GATES'; nexttask='PRESERVE_CHAMPION_AND_STOP_HISTORICAL_CHALLENGER_WORK_TEMPORARILY'
 elif o.brier_improvement<-.002 and o.log_loss_improvement<-.002: classification='CHALLENGER_DEGRADED_CONTROL'; nexttask='PRESERVE_CHAMPION_AND_STOP_HISTORICAL_CHALLENGER_WORK_TEMPORARILY'
 else: classification='CHALLENGER_NO_INCREMENTAL_SIGNAL'; nexttask='PRESERVE_CHAMPION_AND_STOP_HISTORICAL_CHALLENGER_WORK_TEMPORARILY'
 decisions={"NHL_MONEYLINE_CHALLENGER_EXECUTION_CONTRACT_VERIFIED":"READY","NHL_MONEYLINE_CHALLENGER_FEATURE_MATRIX_VERIFIED":"READY","NHL_MONEYLINE_CHALLENGER_SINGLE_FIT_EXECUTED":"READY","NHL_MONEYLINE_CHALLENGER_VALIDATION_RESULT":"PASS" if gates[gates.gate_name.str.startswith('validation')].passed.all() else "FAIL","NHL_MONEYLINE_CHALLENGER_HOLDOUT_RESULT":"PASS" if gates[gates.gate_name.str.startswith('holdout')].passed.all() else "FAIL","NHL_MONEYLINE_CHALLENGER_COMBINED_OOT_RESULT":"PASS" if gates[gates.gate_name.str.startswith('combined')].passed.all() else "FAIL","NHL_MONEYLINE_CHALLENGER_BOOTSTRAP_RESULT":"PASS" if gates[gates.gate_name.str.startswith('bootstrap')].passed.all() else "FAIL","NHL_MONEYLINE_CHALLENGER_STABILITY_RESULT":"PASS" if gates[gates.gate_name.str.contains('month|team|margin|fully')].passed.all() else "FAIL","NHL_MONEYLINE_CHALLENGER_CALIBRATION_RESULT":"PASS" if gates[gates.gate_name.str.contains('ece')].passed.all() else "FAIL","NHL_MONEYLINE_CHALLENGER_INFORMATION_NOVELTY_RESULT":"MIXED_DIRECTIONAL_EFFECTS" if not novelty.sign_aligns_with_hypothesis.all() else "DIRECTIONALLY_COHERENT","NHL_MONEYLINE_CHALLENGER_ALL_FROZEN_GATES":"PASS" if allpass else "FAIL","NHL_MONEYLINE_CHAMPION_CHALLENGER_EXECUTION_DECISION":classification,"NHL_MONEYLINE_MODEL_PROMOTION_READINESS":"NOT_READY","NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS":"NOT_READY"}
 decision={"classification":classification,"all_frozen_gates_passed":allpass,"gate_pass_count":int(gates.passed.sum()),"gate_total":len(gates),"decisions":decisions,"recommended_next_bounded_task":nexttask,"unlocked":["A separately authorized promotion-grade historical evaluation specification"] if allpass else ["Preserve the certified champion as the historical control; record this challenger as completed and failed/partial"],"still_blocked":["odds acquisition","ROI analysis","model promotion","deployment","season 2026 operational restart"],"promotion_boundary":"MODEL_PROMOTION_NOT_AUTHORIZED"}; js(decision,out/f'nhl_moneyline_champion_challenger_execution_decision_{DATE}.json')
 identity={"experiment":"FROZEN_NHL_MONEYLINE_CHAMPION_CHALLENGER_EXECUTION","champion":CHAMP,"challenger":CHALL,"champion_prediction_sha256":CHAMP_HASH,"specification_manifest_sha256":PARENTS['specification'][1],"feature_order":FEATURES,"model":{"family":"logistic_regression","penalty":"l2","C":1.0,"solver":"liblinear","seed":SEED,"max_iter":1000,"tol":.0001},"fit_rows":701,"fit_count":1,"probability_rows":2798,"fitted_parameter_sha256":sha(out/f'nhl_moneyline_challenger_fitted_parameters_{DATE}.csv'),"preprocessing_state":"Embedded in fitted parameter CSV","contract_verified":True}; js(identity,out/f'nhl_moneyline_challenger_execution_identity_{DATE}.json')
 text=f"""# NHL Moneyline Champion–Challenger Execution\n\n## Result\n\n`{classification}`. The frozen challenger passed {int(gates.passed.sum())} of {len(gates)} gates. The champion was not refit; the challenger was fit exactly once.\n\n## Out-of-time comparison\n\nValidation Brier improvement was {v.brier_improvement:.6f}, log-loss improvement {v.log_loss_improvement:.6f}, ROC AUC change {v.roc_auc_change:.6f}, and ECE degradation {v.ece_degradation:.6f}. Holdout values were {h.brier_improvement:.6f}, {h.log_loss_improvement:.6f}, {h.roc_auc_change:.6f}, and {h.ece_degradation:.6f}. Combined out-of-time Brier improvement was {o.brier_improvement:.6f} and log-loss improvement {o.log_loss_improvement:.6f}. Positive proper-score improvement favors the challenger.\n\nBootstrap positive fractions were {bm:.4f} for Brier and {lm:.4f} for log loss. Leave-one-month-out joint improvement fraction was {mf:.4f}; leave-one-team-out was {tf:.4f}. {ng} of five margin bands had nonnegative Brier improvement; after excluding the best band, improvement was {nobest.brier_improvement:.6f}.\n\n## Boundary\n\nNo tuning, alternative workload window, feature removal, refit, or subgroup search was performed. Recommended next bounded task: `{nexttask}`. `MODEL_PROMOTION_NOT_AUTHORIZED`.\n"""
 (out/f'nhl_moneyline_champion_challenger_one_page_summary_{DATE}.md').write_text(text); (out/f'nhl_moneyline_champion_challenger_execution_report_{DATE}.md').write_text(text+'\n## Fitted coefficients\n\n'+'\n'.join(f"- `{r.parameter}`: `{r.coefficient:.9f}`" for _,r in pdf.iterrows())+'\n\n## Required decisions\n\n'+'\n'.join(f'- `{k}` = `{v}`' for k,v in decisions.items())+'\n')
 assert before=={str(f):sha(f) for p in pp.values() for f in p.iterdir() if f.is_file()}
 pkg={"package_name":"nhl_moneyline_champion_challenger_execution","version":"1.0.0","as_of_date":DATE,"generated_by":str(Path(__file__).relative_to(root)),"challenger_fit_count":1,"champion_refit_count":0,"classification":classification,"parent_manifest_sha256":{k:h for k,(_,h) in PARENTS.items()},"source_mutation_check":"PASS","promotion_boundary":"MODEL_PROMOTION_NOT_AUTHORIZED"}; js(pkg,out/f'package_identity_{DATE}.json')
 files=sorted(x for x in out.iterdir() if x.is_file() and x.name!='SHA256SUMS'); (out/'SHA256SUMS').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files)); print(json.dumps({"output_dir":str(out),"classification":classification,"gates":f"{gates.passed.sum()}/{len(gates)}","oot_brier_improvement":o.brier_improvement,"oot_log_loss_improvement":o.log_loss_improvement,"manifest_sha256":sha(out/'SHA256SUMS')},indent=2))
if __name__=='__main__': main()
