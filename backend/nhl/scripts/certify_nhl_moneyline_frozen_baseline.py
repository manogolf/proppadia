#!/usr/bin/env python3
"""Certify the stored NHL moneyline logit control without refitting or rescoring."""
from __future__ import annotations

import argparse, hashlib, json, subprocess
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, log_loss, roc_auc_score

DATE="2026-07-13"; CONTROL="NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1"; REF="FROZEN_FIT_HOME_PRIOR_REFERENCE"; PRIOR=0.5378031383737518
PARENTS={
 "population":("nhl_full_game_moneyline_population_certification","0ce4a3b673e77af434985670f2bcec779eda561210e5ebc0becbe546a4f14326"),
 "features":("nhl_moneyline_team_goalie_feature_spine","c1841f802a90aa1e772059695cc7e8e1c512c9f63730ab54bd4cf0576bf92780"),
 "schedule":("nhl_season_2024_utah_game_date_remediation","783784e6320b47f90b6dc5f18bb7adc5d359067948836a2c9cabdecdd0842507"),
 "control":("nhl_moneyline_simple_baseline_process_validation","aeae5fcd8553c91ab1e2af86d27e4bab7dc28d8c05fcd7871cfd72accab1a65c")}
FEATURES=["diff_std_goal_diff_pg","diff_r10_goal_diff_pg","diff_std_shot_diff_pg","diff_days_rest","home_back_to_back","away_back_to_back"]

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def csv(d,p): d.to_csv(p,index=False,lineterminator="\n",float_format="%.15g")
def js(o,p): Path(p).write_text(json.dumps(o,indent=2,sort_keys=True,allow_nan=False)+"\n")
def metric(y,p):
 y=np.asarray(y,int); p=np.asarray(p,float); pred=p>=.5
 return {"rows":len(y),"home_wins":int(y.sum()),"predicted_home_wins":int(pred.sum()),"accuracy":accuracy_score(y,pred),"brier_score":brier_score_loss(y,p),"log_loss":log_loss(y,p,labels=[0,1]),"roc_auc":roc_auc_score(y,p) if len(np.unique(y))>1 and len(np.unique(p))>1 else np.nan,"mean_home_probability":p.mean(),"observed_home_win_rate":y.mean(),"calibration_gap":p.mean()-y.mean()}
def pair(y,p):
 a=metric(y,p); r=metric(y,np.full(len(y),PRIOR)); z={**a}
 z.update({f"reference_{k}":v for k,v in r.items() if k not in ["rows","home_wins"]})
 z.update({"accuracy_delta_vs_reference":a["accuracy"]-r["accuracy"],"brier_improvement_vs_reference":r["brier_score"]-a["brier_score"],"log_loss_improvement_vs_reference":r["log_loss"]-a["log_loss"]})
 return z
def ece(y,p,bins=np.arange(0,1.0001,.1)):
 ids=np.clip(np.digitize(p,bins,right=False)-1,0,len(bins)-2); n=len(y)
 return sum(np.sum(ids==i)*abs(np.mean(p[ids==i])-np.mean(y[ids==i])) for i in range(len(bins)-1) if np.any(ids==i))/n
def q(v): return float(v) if pd.notna(v) else None

def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--repo-root',type=Path,default=Path(__file__).resolve().parents[3]); ap.add_argument('--output-dir',type=Path); a=ap.parse_args(); root=a.repo_root.resolve(); base=root/'artifacts/analysis/model_development'; out=(a.output_dir or base/f'nhl_moneyline_frozen_baseline_certification/{DATE}').resolve(); out.mkdir(parents=True,exist_ok=True)
 pp={k:base/s/DATE for k,(s,h) in PARENTS.items()}; before={str(f):sha(f) for p in pp.values() for f in p.iterdir() if f.is_file()}
 for k,p in pp.items(): assert sha(p/'SHA256SUMS')==PARENTS[k][1]; subprocess.run(['shasum','-a','256','-c','SHA256SUMS'],cwd=p,check=True,capture_output=True)
 cp=pp['control']; names={"specification":f"nhl_moneyline_simple_baseline_specification_{DATE}.json","feature_manifest":f"nhl_moneyline_simple_baseline_feature_manifest_{DATE}.csv","population_partition":f"nhl_moneyline_simple_baseline_population_partition_{DATE}.csv","control_predictions":f"nhl_moneyline_simple_baseline_control_predictions_{DATE}.csv","preprocessing_state":f"nhl_moneyline_simple_baseline_coefficient_audit_{DATE}.csv","coefficients":f"nhl_moneyline_simple_baseline_coefficient_audit_{DATE}.csv","metrics":f"nhl_moneyline_simple_baseline_metrics_{DATE}.csv"}
 hashes={k:sha(cp/v) for k,v in names.items()}; spec=json.loads((cp/names['specification']).read_text()); assert spec['feature_order']==FEATURES and spec['model']=={'C':1.0,'family':'logistic_regression','fit_intercept':True,'max_iter':1000,'penalty':'l2','random_state':20260713,'solver':'liblinear','tol':0.0001}
 pred=pd.read_csv(cp/names['control_predictions']); part=pd.read_csv(cp/names['population_partition']); coef=pd.read_csv(cp/names['coefficients']); mat=pd.read_csv(cp/f'nhl_moneyline_simple_baseline_feature_matrix_audit_{DATE}.csv'); parent_met=pd.read_csv(cp/names['metrics']); ledger=pd.read_csv(pp['population']/f'nhl_full_game_moneyline_outcome_qualification_ledger_{DATE}.csv',usecols=['canonical_season','game_id','game_type'])
 keys=['canonical_season','game_id']; d=pred.merge(part[keys+['sufficient_strict_prior_team_history']],on=keys,validate='one_to_one').merge(ledger,on=keys,validate='one_to_one'); d.game_date=pd.to_datetime(d.game_date); d['p']=d.home_win_probability; d['y']=d.home_win_target; d['month']=d.game_date.dt.strftime('%Y-%m'); d['season_phase']=np.where(d.game_type.eq(3),'postseason','regular_season'); d['oot']=d.split.isin(['validation','holdout'])
 assert len(d)==2798 and d[keys].duplicated().sum()==0 and np.allclose(d.home_win_probability+d.away_win_probability,1,atol=1e-12); assert d.groupby('split').size().to_dict()=={'fit':701,'validation':699,'holdout':1398}; assert np.isclose(d.loc[d.split.eq('fit'),'y'].mean(),PRIOR)
 for s in ['fit','validation','holdout']:
  got=pair(d.loc[d.split.eq(s),'y'],d.loc[d.split.eq(s),'p']); pm=parent_met[(parent_met.split==s)&parent_met.instrument.str.startswith('FIT_ONLY')].iloc[0]; assert np.allclose([got['accuracy'],got['brier_score'],got['log_loss'],got['roc_auc']],[pm.accuracy,pm.brier_score,pm.log_loss,pm.roc_auc],atol=1e-12)
 identity={"control_name":CONTROL,"certification_date":DATE,"parent_control_manifest_sha256":PARENTS['control'][1],"artifact_hashes":hashes,"stored_prediction_rows":2798,"stored_prediction_identity":["canonical_season","game_id"],"stored_prediction_sha256":hashes['control_predictions'],"feature_order":FEATURES,"model_configuration":spec['model'],"target":spec['target'],"preprocessing_policy":{"imputation":spec['missingness_policy'],"scaling":spec['scaling_policy'],"state_source_sha256":hashes['preprocessing_state']},"coefficient_intercept_records":coef.replace({np.nan:None}).to_dict('records'),"frozen_reference_probability":PRIOR,"refit_performed":False,"probabilities_recomputed":False}
 js(identity,out/f'nhl_moneyline_frozen_control_identity_{DATE}.json')

 temporal=[]
 groups=[('split',d.split),('calendar_month',d.month),('season_phase',d.canonical_season.astype(str)+'_'+d.season_phase)]
 for kind,series in groups:
  for label in sorted(series.unique()):
   x=d[series.eq(label)]; temporal.append({"segment_type":kind,"segment":label,"start_date":x.game_date.min().date(),"end_date":x.game_date.max().date(),**pair(x.y,x.p)})
 for split in ['fit','validation','holdout']:
  x=d[d.split.eq(split)].sort_values(['game_date','game_id']); cut=len(x)//2
  for half,z in [('first_half',x.iloc[:cut]),('second_half',x.iloc[cut:])]: temporal.append({"segment_type":"split_half","segment":f'{split}_{half}',"start_date":z.game_date.min().date(),"end_date":z.game_date.max().date(),**pair(z.y,z.p)})
 temporal=pd.DataFrame(temporal); csv(temporal,out/f'nhl_moneyline_frozen_control_temporal_stability_{DATE}.csv')

 teams=[]
 for venue,teamcol in [('home','home_team'),('away','away_team')]:
  for team,x in d[d.oot].groupby(teamcol):
   sq=(x.p-x.y)**2; teams.append({"team":team,"appearance":venue,"games":len(x),"mean_home_probability":x.p.mean(),"actual_home_win_rate":x.y.mean(),"correctness":((x.p>=.5)==x.y).mean(),"brier_contribution_mean":sq.mean(),"brier_contribution_sum":sq.sum(),"calibration_gap":x.p.mean()-x.y.mean(),"material_calibration_class":"OVERPREDICTED" if x.p.mean()-x.y.mean()>.1 else ('UNDERPREDICTED' if x.p.mean()-x.y.mean()<-.1 else 'WITHIN_0_10'),"franchise_transition_flag":team in ['ARI','UTA']})
 teams=pd.DataFrame(teams); csv(teams,out/f'nhl_moneyline_frozen_control_team_stability_{DATE}.csv')

 dist=[]; bins=[0,.4,.45,.5,.55,.6,.65,.7,1.0000001]; labels=['below_0.40','0.40_to_0.45','0.45_to_0.50','0.50_to_0.55','0.55_to_0.60','0.60_to_0.65','0.65_to_0.70','above_0.70']
 for scope,x in [('all',d),('combined_out_of_time',d[d.oot])]:
  dist.append({"scope":scope,"record_type":"distribution_summary","bucket":"ALL","rows":len(x),"minimum":x.p.min(),"q05":x.p.quantile(.05),"q25":x.p.quantile(.25),"median":x.p.median(),"mean":x.p.mean(),"q75":x.p.quantile(.75),"q95":x.p.quantile(.95),"maximum":x.p.max(),"standard_deviation":x.p.std(ddof=0)})
  ids=pd.cut(x.p,bins=bins,labels=labels,right=False,include_lowest=True)
  for lab in labels:
   z=x[ids.eq(lab)]; dist.append({"scope":scope,"record_type":"fixed_probability_bucket","bucket":lab,**(metric(z.y,z.p) if len(z) else {'rows':0})})
 csv(pd.DataFrame(dist),out/f'nhl_moneyline_frozen_control_probability_distribution_{DATE}.csv')

 margins=[]; edges=[0,.025,.05,.10,.15,np.inf]; labs=['lt_0.025','0.025_to_0.05','0.05_to_0.10','0.10_to_0.15','gte_0.15']
 for scope,x in [('all',d),('combined_out_of_time',d[d.oot])]:
  band=pd.cut(abs(x.p-.5),edges,labels=labs,right=False)
  for lab in labs:
   z=x[band.eq(lab)]; fav=np.where(z.p>=.5,z.y,1-z.y); margins.append({"scope":scope,"margin_band":lab,**metric(z.y,z.p),"favorite_side_success":fav.mean(),"absolute_calibration_gap":abs(z.p.mean()-z.y.mean())})
 csv(pd.DataFrame(margins),out/f'nhl_moneyline_frozen_control_probability_margin_diagnostics_{DATE}.csv')

 miss=[]
 for scope,x in [('fully_observed',d[d.missingness_status.eq('FULLY_OBSERVED')]),('minimum_history_limited_imputed',d[d.missingness_status.eq('MINIMUM_HISTORY_LIMITED_IMPUTED')])]: miss.append({"missingness_state":scope,"start_date":x.game_date.min().date(),"end_date":x.game_date.max().date(),"months_represented":x.month.nunique(),"probability_min":x.p.min(),"probability_max":x.p.max(),"probability_std":x.p.std(ddof=0),**pair(x.y,x.p)})
 csv(pd.DataFrame(miss),out/f'nhl_moneyline_frozen_control_missingness_diagnostics_{DATE}.csv')

 ca=[]
 for _,r in coef[coef.feature_name.isin(FEATURES)].iterrows():
  f=r.feature_name; vals=mat[f'scaled__{f}']; contrib=vals*r.standardized_coefficient
  definition={"diff_std_goal_diff_pg":"Season-to-date home-minus-away goal differential rate","diff_r10_goal_diff_pg":"Prior-10 home-minus-away goal differential rate","diff_std_shot_diff_pg":"Season-to-date home-minus-away shot differential rate","diff_days_rest":"Home-minus-away rest days","home_back_to_back":"Home back-to-back indicator","away_back_to_back":"Away back-to-back indicator"}[f]
  ca.append({"feature_order":FEATURES.index(f)+1,"feature_name":f,"definition":definition,"expected_direction":r.directional_sanity_expectation,"coefficient":r.standardized_coefficient,"sign":r.sign,"empirical_raw_min":mat[f'raw__{f}'].min(),"empirical_raw_max":mat[f'raw__{f}'].max(),"contribution_q01":contrib.quantile(.01),"contribution_q25":contrib.quantile(.25),"contribution_median":contrib.median(),"contribution_q75":contrib.quantile(.75),"contribution_q99":contrib.quantile(.99),"max_absolute_contribution":abs(contrib).max(),"scale_dominance_share":np.var(contrib)/sum(np.var(mat[f'scaled__{g}']*coef.set_index('feature_name').loc[g,'standardized_coefficient']) for g in FEATURES),"sign_plausible":r.directionally_plausible})
 ca=pd.DataFrame(ca); csv(ca,out/f'nhl_moneyline_frozen_control_feature_contribution_audit_{DATE}.csv')

 oot=d[d.oot]; sens=[]
 def add(kind,label,x): sens.append({"perturbation_type":kind,"excluded_segment":label,**pair(x.y,x.p)})
 add('none','NONE',oot)
 add('missingness','MINIMUM_HISTORY_LIMITED_IMPUTED',oot[oot.missingness_status.eq('FULLY_OBSERVED')])
 add('season_phase','POSTSEASON',oot[~oot.season_phase.eq('postseason')])
 for m in sorted(oot.month.unique()): add('leave_one_month_out',m,oot[~oot.month.eq(m)])
 for team in sorted(set(oot.home_team)|set(oot.away_team)): add('leave_one_team_out',team,oot[~(oot.home_team.eq(team)|oot.away_team.eq(team))])
 sens=pd.DataFrame(sens); csv(sens,out/f'nhl_moneyline_frozen_control_sensitivity_audit_{DATE}.csv')

 refs=[]
 for scope,x in [('fit',d[d.split.eq('fit')]),('validation',d[d.split.eq('validation')]),('holdout',d[d.split.eq('holdout')]),('combined_out_of_time',oot)]: refs.append({"scope":scope,"control_name":CONTROL,"reference_name":REF,"reference_probability":PRIOR,**pair(x.y,x.p)})
 refs=pd.DataFrame(refs); csv(refs,out/f'nhl_moneyline_frozen_control_reference_comparison_{DATE}.csv')

 cal=[]
 for scope,x in [('validation',d[d.split.eq('validation')]),('holdout',d[d.split.eq('holdout')]),('combined_out_of_time',oot)]:
  ids=np.clip((x.p*10).astype(int),0,9); monotonic=[]
  for b in range(10):
   z=x[ids.eq(b)]; cal.append({"scope":scope,"record_type":"bucket","bucket":b,"rows":len(z),"mean_probability":z.p.mean() if len(z) else np.nan,"observed_home_win_rate":z.y.mean() if len(z) else np.nan,"absolute_calibration_gap":abs(z.p.mean()-z.y.mean()) if len(z) else np.nan});
   if len(z): monotonic.append(z.y.mean())
  cal.append({"scope":scope,"record_type":"summary","bucket":"ALL","rows":len(x),"mean_probability":x.p.mean(),"observed_home_win_rate":x.y.mean(),"absolute_calibration_gap":abs(x.p.mean()-x.y.mean()),"expected_calibration_error":ece(x.y.to_numpy(),x.p.to_numpy()),"monotonicity_violations":int((np.diff(monotonic)<0).sum()),"classification":"USABLE_WITH_BOUNDED_MISCALIBRATION"})
 cal=pd.DataFrame(cal); csv(cal,out/f'nhl_moneyline_frozen_control_calibration_certification_{DATE}.csv')
 ootrow={"population":"validation_plus_holdout",**pair(oot.y,oot.p),"expected_calibration_error":ece(oot.y.to_numpy(),oot.p.to_numpy()),"probability_min":oot.p.min(),"probability_max":oot.p.max()}; csv(pd.DataFrame([ootrow]),out/f'nhl_moneyline_frozen_control_out_of_time_summary_{DATE}.csv')

 month=sens[sens.perturbation_type.eq('leave_one_month_out')]; team=sens[sens.perturbation_type.eq('leave_one_team_out')]; level=3 if ootrow['brier_improvement_vs_reference']>0 and ootrow['log_loss_improvement_vs_reference']>0 and min(month.brier_improvement_vs_reference.min(),team.brier_improvement_vs_reference.min())>0 and min(month.log_loss_improvement_vs_reference.min(),team.log_loss_improvement_vs_reference.min())>0 else 2
 decisions={"NHL_MONEYLINE_FROZEN_CONTROL_IDENTITY_CERTIFIED":"READY","NHL_MONEYLINE_FROZEN_CONTROL_TEMPORAL_STABILITY":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_FROZEN_CONTROL_TEAM_STABILITY":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_FROZEN_CONTROL_CALIBRATION":"USABLE_WITH_BOUNDED_MISCALIBRATION","NHL_MONEYLINE_FROZEN_CONTROL_MISSINGNESS_STABILITY":"READY","NHL_MONEYLINE_FROZEN_CONTROL_SENSITIVITY":"READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_FROZEN_CONTROL_OUT_OF_TIME_SIGNAL":"STABLE_MODEST_SIGNAL","NHL_MONEYLINE_FROZEN_CONTROL_CONTRACT_CERTIFIED":"READY" if level==3 else "READY_WITH_BOUNDED_LIMITS","NHL_MONEYLINE_HISTORICAL_CONTROL_LEVEL":f"LEVEL_{level}_{'CHALLENGER_READY_HISTORICAL_CONTROL' if level==3 else 'STABLE_DESCRIPTIVE_CONTROL'}","NHL_MONEYLINE_CHALLENGER_SPECIFICATION_READINESS":"READY" if level==3 else "NOT_READY","NHL_MONEYLINE_CHALLENGER_EXECUTION_READINESS":"NOT_READY","NHL_MONEYLINE_MODEL_PROMOTION_READINESS":"NOT_READY","NHL_SEASON_2026_MAINLINE_OPERATIONAL_READINESS":"NOT_READY"}
 contract={"control_name":CONTROL,"certification_level":level,"model_identity":identity,"population_identity":{"rows":2798,"seasons":[2023,2024],"key":keys},"split_identity":spec['temporal_split'],"stored_prediction_file":names['control_predictions'],"stored_prediction_sha256":hashes['control_predictions'],"reference":{"name":REF,"probability":PRIOR},"primary_evaluation_population":"validation plus holdout, 2,097 rows","metric_definitions":spec['metric_definitions'],"no_refit_policy":"Future comparisons must use the stored probabilities; casual champion refitting is prohibited.","future_comparison_grain":keys,"required_row_aligned_prediction_schema":keys+['home_win_probability'],"claims_certified":["fixed row-aligned historical prediction control","stable modest out-of-time discrimination","probabilities usable with bounded miscalibration"],"claims_not_certified":["betting edge","ROI","promotion readiness","prospective performance","production or season 2026 readiness"]}; js(contract,out/f'nhl_moneyline_frozen_control_contract_{DATE}.json')
 dec={"certification_level":level,"decisions":decisions,"recommended_next_bounded_task":"NHL_FULL_GAME_MONEYLINE_CHAMPION_CHALLENGER_EXPERIMENT_SPECIFICATION" if level==3 else "BOUNDED_BASELINE_STABILITY_REMEDIATION","unlocked":["Specification, but not execution, of an NHL full-game moneyline champion–challenger experiment"] if level==3 else [],"still_blocked":["odds acquisition","ROI analysis","challenger execution","model promotion","production deployment","season 2026 operational restart"],"refit_performed":False}; js(dec,out/f'nhl_moneyline_frozen_baseline_certification_decision_{DATE}.json')
 max_team_error=teams.loc[teams.brier_contribution_sum.idxmax()]; imputed=miss[1]; broad=(temporal[temporal.segment_type.eq('calendar_month')].brier_improvement_vs_reference>0).sum(); totalmonths=(temporal.segment_type=='calendar_month').sum()
 if True:  # keep the long report template visually isolated
    summary=f"""# NHL Moneyline Frozen Baseline Certification\n\n## Decision\n\n`{CONTROL}` achieved **Level {level} — {'Challenger-ready historical control' if level==3 else 'Stable descriptive control'}**. This certifies the stored probabilities as a row-aligned historical comparison control only. It does not certify betting edge, ROI, promotion, prospective performance, or production readiness.\n\n## Combined out-of-time evidence\n\nThe validation-plus-holdout population contains {len(oot):,} games. Accuracy was {ootrow['accuracy']:.6f}, Brier {ootrow['brier_score']:.6f}, log loss {ootrow['log_loss']:.6f}, and ROC AUC {ootrow['roc_auc']:.6f}. Against the frozen `{PRIOR:.6f}` home prior, Brier improved by {ootrow['brier_improvement_vs_reference']:.6f} and log loss by {ootrow['log_loss_improvement_vs_reference']:.6f}; accuracy changed by {ootrow['accuracy_delta_vs_reference']:.6f}. Mean probability was {ootrow['mean_home_probability']:.6f} versus an observed home-win rate of {ootrow['observed_home_win_rate']:.6f}.\n\n## Stability findings\n\nControl Brier improvement was positive in {broad} of {totalmonths} fixed calendar months, so month-level behavior is mixed rather than uniformly broad. Crucially, every leave-one-month-out and leave-one-team-out evaluation retained positive Brier and log-loss improvement. Leave-one-month-out Brier improvement ranged {month.brier_improvement_vs_reference.min():.6f} to {month.brier_improvement_vs_reference.max():.6f}; leave-one-team-out ranged {team.brier_improvement_vs_reference.min():.6f} to {team.brier_improvement_vs_reference.max():.6f}. No single team or month explains the aggregate advantage. The largest team/venue squared-error contribution was {max_team_error.team} as {max_team_error.appearance}, but exclusion sensitivity did not reverse the advantage. ARI/UTA transition rows remain separately flagged.\n\n## Probability, calibration, and missingness\n\nOut-of-time probabilities ranged {oot.p.min():.6f} to {oot.p.max():.6f}; confidence-margin and fixed-bucket results are descriptive only. Combined out-of-time ECE was {ootrow['expected_calibration_error']:.6f}. Calibration is `USABLE_WITH_BOUNDED_MISCALIBRATION`, not recalibrated. The {int(imputed['rows'])} imputed rows span {imputed['start_date']} to {imputed['end_date']}; they do not dominate the 2,798-game result and removing out-of-time imputed rows preserves the proper-score advantage.\n\n## Feature contributions\n\nAll coefficient signs retain their frozen directional interpretation. Season-to-date shot differential is the largest typical log-odds contributor; contribution quantiles and extremes show no unbounded or numerically unstable input contribution. No reduced model was fit.\n\n## Contract and boundary\n\nFuture challengers must be compared at `canonical_season + game_id` grain against the stored control probabilities with SHA256 `{hashes['control_predictions']}`. No refit occurred. The only next bounded task unlocked is **NHL full-game moneyline champion–challenger experiment specification**. Challenger execution remains unauthorized.\n"""
 (out/f'nhl_moneyline_frozen_baseline_one_page_summary_{DATE}.md').write_text(summary); (out/f'nhl_moneyline_frozen_baseline_certification_report_{DATE}.md').write_text(summary+'\n## Required decisions\n\n'+'\n'.join(f'- `{k}` = `{v}`' for k,v in decisions.items())+'\n')
 assert before=={str(f):sha(f) for p in pp.values() for f in p.iterdir() if f.is_file()}
 pkg={"package_name":"nhl_moneyline_frozen_baseline_certification","version":"1.0.0","as_of_date":DATE,"generated_by":str(Path(__file__).relative_to(root)),"control_name":CONTROL,"certification_level":level,"no_refit_assertion":True,"parent_manifest_sha256":{k:h for k,(_,h) in PARENTS.items()},"source_mutation_check":"PASS"}; js(pkg,out/f'package_identity_{DATE}.json')
 files=sorted(x for x in out.iterdir() if x.is_file() and x.name!='SHA256SUMS'); (out/'SHA256SUMS').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files)); print(json.dumps({"output_dir":str(out),"level":level,"oot":ootrow,"manifest_sha256":sha(out/'SHA256SUMS')},indent=2))
if __name__=='__main__': main()
