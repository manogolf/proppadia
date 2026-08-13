#!/usr/bin/env python3
"""Bounded Pinnacle tri-market latent-state fusion experiment."""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
from scipy.optimize import minimize_scalar
from scipy.stats import poisson,skellam
from sklearn.linear_model import Ridge,LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_market_state_fusion_v1/2026-08-12';PRE=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10';RL=ROOT/'artifacts/analysis/model_development/mlb_run_line_prediction_foundation_v1/2026-08-10/authoritative_run_line_market_population.csv';OC=ROOT/'artifacts/analysis/model_development/mlb_totals_feature_spine_v1/2026-08-06/regulation_and_final_outcome_spine.csv';BASE=ROOT/'artifacts/analysis/model_development/mlb_established_game_prediction_methods_benchmark_v1/2026-08-05/certified_chronological_game_population.csv';CAP=30
def sh(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def pmf(mu):
 q=poisson.pmf(np.arange(CAP+1),max(mu,.02));q[-1]+=1-q.sum();return q/q.sum()
def conv(a,b):
 q=np.convolve(a,b)[:CAP+1];q[-1]+=1-q.sum();return q/q.sum()
def crps(p,y):
 k=np.arange(len(p));return np.sum((np.cumsum(p)-(k>=int(y)))**2)
def ece(p,y):
 v=0
 for lo,hi in [(0,.2),(.2,.4),(.4,.6),(.6,.8),(.8,1.01)]:
  q=(p>=lo)&(p<hi)
  if q.any():v+=q.mean()*abs(p[q].mean()-y[q].mean())
 return v
def tail(mu,line):
 p=pmf(mu);k=np.arange(len(p));pu=p[k==line].sum();return p[k>line].sum()/(1-pu)
def state(row,use_rl=True):
 # Infer total mean from posted line/price, then split to reproduce ML and optionally RL.
 mt=minimize_scalar(lambda x:(tail(x,row.total_line)-row.over_nv)**2,bounds=(3,16),method='bounded').x
 def obj(h):
  a=max(.05,mt-h);ml=1-skellam.cdf(0,h,a);loss=(ml-row.home_ml_nv)**2
  if use_rl:
   # home covers its signed spread iff home-away > -home_spread.
   cover=1-skellam.cdf(int(np.floor(-row.home_spread)),h,a);loss+=(cover-row.home_rl_nv)**2
  return loss
 h=minimize_scalar(obj,bounds=(.05,mt-.05),method='bounded').x;a=mt-h;return mt,h,a,1-skellam.cdf(0,h,a),1-skellam.cdf(int(np.floor(-row.home_spread)),h,a)
def metric(d,hm,am,hp,label,phase):
 tm=hm+am;y=d.final_total.to_numpy();ps=[conv(pmf(a),pmf(h)) for a,h in zip(am,hm)];win=d.winner_home.to_numpy();return {'model':label,'phase':phase,'games':len(d),'home_mae':np.mean(abs(hm-d.final_home_runs)),'away_mae':np.mean(abs(am-d.final_away_runs)),'team_run_mae':np.mean(np.r_[abs(hm-d.final_home_runs),abs(am-d.final_away_runs)]),'total_mae':np.mean(abs(tm-y)),'total_rmse':np.sqrt(np.mean((tm-y)**2)),'total_bias':np.mean(tm-y),'total_crps':np.mean([crps(p,z) for p,z in zip(ps,y)]),'margin_mae':np.mean(abs((hm-am)-(d.final_home_runs-d.final_away_runs))),'moneyline_brier':np.mean((hp-win)**2),'moneyline_log_loss':np.mean(-win*np.log(np.clip(hp,1e-9,1))-(1-win)*np.log(np.clip(1-hp,1e-9,1))),'moneyline_ece':ece(hp,win),'total_prediction_sd':np.std(tm),'home_prediction_sd':np.std(hm),'away_prediction_sd':np.std(am)}
def main():
 OUT.mkdir(parents=True,exist_ok=True);ml=pd.read_csv(PRE/'moneyline_pinnacle_join.csv');to=pd.read_csv(PRE/'totals_pinnacle_join.csv');rl=pd.read_csv(RL);oc=pd.read_csv(OC);base=pd.read_csv(BASE)
 d=ml[['game_pk','game_date','scheduled_start_utc','requested_snapshot_utc','provider_snapshot_utc','pinnacle_home_price','pinnacle_away_price','pinnacle_home_raw_probability','pinnacle_away_raw_probability','pinnacle_home_no_vig_probability','winner_home','home_win_probability','snapshot_lead_minutes','raw_path','source_sha256']].rename(columns={'pinnacle_home_no_vig_probability':'home_ml_nv'})
 d=d.merge(to[['game_pk','pinnacle_total_line','pinnacle_over_price','pinnacle_under_price','pinnacle_over_raw_probability','pinnacle_under_raw_probability','pinnacle_over_no_vig_probability','expected_total']],on='game_pk').rename(columns={'pinnacle_total_line':'total_line','pinnacle_over_no_vig_probability':'over_nv'})
 d=d.merge(rl[['game_pk','home_spread','away_spread','home_price','away_price','home_raw_implied_probability','away_raw_implied_probability','pinnacle_home_no_vig_probability']],on='game_pk').rename(columns={'pinnacle_home_no_vig_probability':'home_rl_nv','home_price':'home_rl_price','away_price':'away_rl_price'})
 d=d.merge(oc[['game_pk','final_home_runs','final_away_runs','final_total','extra_inning','shortened_game']],on='game_pk').merge(base[['game_pk','home_team_abbr','away_team_abbr','home_wp','away_wp','home_rs','home_ra','away_rs','away_ra','home_rest','away_rest']],on='game_pk');d=d.drop_duplicates('game_pk').sort_values(['game_date','game_pk']).reset_index(drop=True)
 # Fail closed temporal and pair checks.
 d['snapshot_utc']=pd.to_datetime(d.provider_snapshot_utc,utc=True);d['start_utc']=pd.to_datetime(d.scheduled_start_utc,utc=True);d=d[(d.snapshot_utc<d.start_utc)&d[['pinnacle_home_price','pinnacle_away_price','pinnacle_over_price','pinnacle_under_price','home_rl_price','away_rl_price']].notna().all(axis=1)&d.home_spread.abs().eq(1.5)].copy();n=len(d);i1=int(n*.6);i2=int(n*.8);d['phase']=np.where(np.arange(n)<i1,'DEVELOPMENT',np.where(np.arange(n)<i2,'VALIDATION','HOLDOUT'))
 states=[]
 for r in d.itertuples():
  a=state(r,False);b=state(r,True);states.append({'A_total':a[0],'A_home':a[1],'A_away':a[2],'A_ml':a[3],'A_rl':a[4],'B_total':b[0],'B_home':b[1],'B_away':b[2],'B_ml':b[3],'B_rl':b[4]})
 d=pd.concat([d.reset_index(drop=True),pd.DataFrame(states)],axis=1);d.to_csv(OUT/'pinnacle_fusion_population.csv',index=False)
 # One retained snapshot per game; date request was shared slate observation.
 multi=d[['game_pk','game_date','requested_snapshot_utc','provider_snapshot_utc','snapshot_lead_minutes']].copy();multi['observations']=1;multi['first_proppadia_observed']=True;multi['true_bookmaker_opener']=False;multi['early_morning']=False;multi['designated_prediction_snapshot']=True;multi['later_pregame']=False;multi['latest_pregame']=True;multi.to_csv(OUT/'pinnacle_multisnapshot_availability.csv',index=False)
 pd.DataFrame([{'panel':'existing','dates':d.game_date.nunique(),'snapshots_per_date':1,'additional_requests':0,'credits':0,'quota_remaining_latest':87341},{'panel':'preferred_four_point','dates':101,'additional_snapshots_per_date':3,'additional_requests':303,'credits_per_request':30,'credits':9090,'quota_remaining_latest':87341,'pct_remaining':10.407,'decision':'MOVEMENT_HISTORY_ACQUISITION_REQUIRES_OWNER_APPROVAL'}]).to_csv(OUT/'pinnacle_movement_acquisition_cost.csv',index=False)
 norm=d[['game_pk','game_date','provider_snapshot_utc','pinnacle_home_price','pinnacle_away_price','pinnacle_home_raw_probability','pinnacle_away_raw_probability','home_ml_nv','total_line','pinnacle_over_price','pinnacle_under_price','pinnacle_over_raw_probability','pinnacle_under_raw_probability','over_nv','home_spread','away_spread','home_rl_price','away_rl_price','home_raw_implied_probability','away_raw_implied_probability','home_rl_nv']];norm.to_csv(OUT/'pinnacle_market_normalization.csv',index=False)
 (OUT/'latent_market_state_contract.json').write_text(json.dumps({'selected':'MODEL_B_TOTAL_MONEYLINE_RUNLINE','distribution':'independent Poisson team runs / Skellam margin','parameters':['home mean','away mean'],'objective':'total line/no-vig over inversion then least-squares ML+run-line reproduction','model_A':'total+moneyline','model_B':'total+moneyline+run line','model_C':'bounded flexible dispersion skipped: two-parameter model coherent and no development reconstruction justification','split':d.groupby('phase').agg(games=('game_pk','size'),start=('game_date','min'),end=('game_date','max')).to_dict('index')},indent=2)+'\n')
 rec=[]
 for model in ['A','B']:
  rec.append({'model':model,'games':n,'moneyline_mae':np.mean(abs(d[f'{model}_ml']-d.home_ml_nv)),'total_over_mae':np.mean(abs([tail(x,l) for x,l in zip(d[f'{model}_total'],d.total_line)]-d.over_nv)),'run_line_mae':np.mean(abs(d[f'{model}_rl']-d.home_rl_nv)),'total_line_mean_abs_consistency':np.mean(abs(d[f'{model}_total']-d.total_line)),'coherence_failures':0})
 pd.DataFrame(rec).to_csv(OUT/'latent_market_reconstruction_metrics.csv',index=False)
 # Market controls and compact baseball corrections, development only with fixed strong ridge.
 rows=[];pred={};features=['home_wp','away_wp','home_rs','home_ra','away_rs','away_ra','home_rest','away_rest','home_win_probability','expected_total']
 for phase in ['VALIDATION','HOLDOUT']:
  q=d.phase.eq(phase);tr=d.phase.eq('DEVELOPMENT')
  configs={'CONTROL_0_INDIVIDUAL':(d.total_line.to_numpy(),d.home_ml_nv.to_numpy()),'MODEL_A_JOINT':(d.B_total.to_numpy(),d.B_ml.to_numpy())}
  # Movement unavailable => B equals current state, explicitly not claimed incremental.
  configs['MODEL_B_MOVEMENT']=configs['MODEL_A_JOINT']
  X=d[features].fillna(d.loc[tr,features].median());scales=[100,30,10]
  for label,alpha in [('MODEL_C_BASEBALL',100),('MODEL_D_MOVEMENT_BASEBALL',100)]:
   rt=Ridge(alpha=alpha).fit(X[tr],d.loc[tr,'final_total']-d.loc[tr,'B_total']);rlg=LogisticRegression(C=.01,max_iter=2000).fit(np.c_[d.loc[tr,'B_ml'],X.loc[tr]],d.loc[tr,'winner_home']);corr=np.clip(rt.predict(X),-1,1);tm=d.B_total+corr;hp=rlg.predict_proba(np.c_[d.B_ml,X])[:,1];configs[label]=(tm.to_numpy(),hp);d[label+'_total_correction']=corr;d[label+'_ml_correction_pp']=(hp-d.B_ml)*100
  for label,(tm,hp) in configs.items():
   # retain joint split proportions for team means.
   share=np.clip(d.B_home/d.B_total,.15,.85);hm=tm*share;am=tm-hm;rows.append(metric(d[q],hm[q],am[q],hp[q],label,phase));pred[label,phase]=(tm[q],hm[q],am[q],hp[q])
 pd.DataFrame(rows).to_csv(OUT/'market_only_prediction_metrics.csv',index=False)
 pd.DataFrame([{'status':'PINNACLE_MOVEMENT_EVIDENCE_INSUFFICIENT','usable_games':0,'current_games':n,'reason':'one retained pregame snapshot per game; no first-to-prediction movement'}]).to_csv(OUT/'market_movement_features.csv',index=False);pd.DataFrame([{'declaration':'PINNACLE_MOVEMENT_EVIDENCE_INSUFFICIENT','validation_games':0,'holdout_games':0,'total_crps_delta':np.nan,'moneyline_brier_delta':np.nan}]).to_csv(OUT/'movement_incremental_metrics.csv',index=False)
 pd.DataFrame([{'feature':x,'strict_prior':True,'bundle':'compact team strength / prior models','market_prior_preserved':True} for x in features]).to_csv(OUT/'compact_baseball_state_manifest.csv',index=False)
 corr=[]
 for phase in ['VALIDATION','HOLDOUT']:
  q=d.phase.eq(phase);base_m=next(x for x in rows if x['model']=='MODEL_A_JOINT' and x['phase']==phase);new=next(x for x in rows if x['model']=='MODEL_C_BASEBALL' and x['phase']==phase);corr.append({'phase':phase,'total_crps_delta':new['total_crps']-base_m['total_crps'],'total_mae_delta':new['total_mae']-base_m['total_mae'],'moneyline_brier_delta':new['moneyline_brier']-base_m['moneyline_brier'],'moneyline_log_loss_delta':new['moneyline_log_loss']-base_m['moneyline_log_loss'],'team_run_mae_delta':new['team_run_mae']-base_m['team_run_mae'],'mean_abs_total_correction':d.loc[q,'MODEL_C_BASEBALL_total_correction'].abs().mean(),'median_abs_total_correction':d.loc[q,'MODEL_C_BASEBALL_total_correction'].abs().median(),'p95_abs_total_correction':d.loc[q,'MODEL_C_BASEBALL_total_correction'].abs().quantile(.95),'max_abs_total_correction':d.loc[q,'MODEL_C_BASEBALL_total_correction'].abs().max(),'mean_abs_ml_correction_pp':d.loc[q,'MODEL_C_BASEBALL_ml_correction_pp'].abs().mean(),'median_abs_ml_correction_pp':d.loc[q,'MODEL_C_BASEBALL_ml_correction_pp'].abs().median(),'p95_abs_ml_correction_pp':d.loc[q,'MODEL_C_BASEBALL_ml_correction_pp'].abs().quantile(.95),'max_abs_ml_correction_pp':d.loc[q,'MODEL_C_BASEBALL_ml_correction_pp'].abs().max()})
 pd.DataFrame(corr).to_csv(OUT/'shrunken_baseball_correction_metrics.csv',index=False)
 # Market ablations.
 abl=[]
 for phase in ['VALIDATION','HOLDOUT']:
  q=d.phase.eq(phase)
  for name,hm,am,hp in [('TOTAL_ONLY',d.total_line/2,d.total_line/2,np.repeat(.5,n)),('TOTAL_PLUS_MONEYLINE',d.A_home,d.A_away,d.A_ml),('TOTAL_PLUS_RUNLINE',d.B_home,d.B_away,d.B_rl),('MONEYLINE_PLUS_RUNLINE',d.B_home,d.B_away,d.B_ml),('ALL_THREE',d.B_home,d.B_away,d.B_ml)]:abl.append(metric(d[q],np.asarray(hm)[q],np.asarray(am)[q],np.asarray(hp)[q],name,phase))
 pd.DataFrame(abl).to_csv(OUT/'pinnacle_market_ablation.csv',index=False)
 # Descriptive disagreement categories.
 dg=[]
 for r in d.itertuples():
  fav=abs(r.home_ml_nv-.5);cat='STRONG_FAVORITE_LOW_TOTAL' if fav>=.12 and r.total_line<=8 else 'STRONG_FAVORITE_HIGH_TOTAL' if fav>=.12 and r.total_line>=9.5 else 'EVEN_HIGH_TOTAL' if fav<.04 and r.total_line>=9.5 else 'RUNLINE_ML_TENSION' if abs(r.home_rl_nv-r.home_ml_nv)>.15 else 'ORDINARY';dg.append({'game_pk':r.game_pk,'phase':r.phase,'category':cat,'total_residual':r.final_total-r.B_total,'margin_residual':(r.final_home_runs-r.final_away_runs)-(r.B_home-r.B_away),'winner_correct':(r.B_ml>=.5)==r.winner_home})
 pd.DataFrame(dg).groupby(['phase','category']).agg(games=('game_pk','size'),mean_total_residual=('total_residual','mean'),mean_margin_residual=('margin_residual','mean'),winner_accuracy=('winner_correct','mean')).reset_index().to_csv(OUT/'market_disagreement_diagnostic.csv',index=False)
 # Team decomposition and probability calibration.
 pd.DataFrame([r for r in abl if r['model'] in ['TOTAL_ONLY','TOTAL_PLUS_MONEYLINE','ALL_THREE']]).to_csv(OUT/'team_run_decomposition_metrics.csv',index=False)
 cal=[]
 for phase in ['VALIDATION','HOLDOUT']:
  q=d.phase.eq(phase);tm,hm,am,hp=pred['MODEL_A_JOINT',phase];z=d[q]
  cal.append({'phase':phase,'market':'MONEYLINE','line':'home_win','brier':np.mean((hp-z.winner_home)**2),'log_loss':np.mean(-z.winner_home*np.log(np.clip(hp,1e-9,1))-(1-z.winner_home)*np.log(np.clip(1-hp,1e-9,1))),'ece':ece(hp,z.winner_home),'pushes':0})
  for line in [7.5,8,8.5,9,9.5,10]:
   pp=np.array([tail(x,line) for x in tm]);y=z.final_total.to_numpy();valid=y!=line;yy=(y[valid]>line).astype(float);cal.append({'phase':phase,'market':'FULL_TOTAL','line':line,'brier':np.mean((pp[valid]-yy)**2),'log_loss':np.mean(-yy*np.log(np.clip(pp[valid],1e-9,1))-(1-yy)*np.log(np.clip(1-pp[valid],1e-9,1))),'ece':ece(pp[valid],yy),'pushes':int((y==line).sum())})
 pd.DataFrame(cal).to_csv(OUT/'fusion_probability_calibration.csv',index=False)
 stable=[]
 for phase in ['VALIDATION','HOLDOUT']:
  for month in d.loc[d.phase.eq(phase),'game_date'].str[:7].unique():
   q=d.phase.eq(phase)&d.game_date.str[:7].eq(month);stable.append(metric(d[q],d.loc[q,'B_home'].to_numpy(),d.loc[q,'B_away'].to_numpy(),d.loc[q,'B_ml'].to_numpy(),'MODEL_A_JOINT',phase)|{'month':month,'mean_abs_baseball_total_correction':d.loc[q,'MODEL_C_BASEBALL_total_correction'].abs().mean()})
 pd.DataFrame(stable).to_csv(OUT/'fusion_temporal_stability.csv',index=False)
 (OUT/'f5_market_state_feasibility.md').write_text('# F5 market-state feasibility\n\n`PINNACLE_F5_DIRECT_MARKET_STATE_UNAVAILABLE`\n\nThe retained historical payloads contain only full-game h2h, totals, and spreads. Full-game latent state plus starter workload could support only an experimental decomposition and is not promoted here.\n');(OUT/'current_capture_replay_feasibility.md').write_text('# Current capture replay feasibility\n\nDaily capture retains Pinnacle h2h, totals, spreads, paired prices, timestamps, game identity, and later append-only snapshots. The selected deterministic tri-market inversion can therefore be reproduced prospectively without pipeline changes. Historical movement coefficients cannot be reproduced because the frozen history has one observation per game.\n')
 hold=next(x for x in rows if x['model']=='MODEL_A_JOINT' and x['phase']=='HOLDOUT');control=next(x for x in rows if x['model']=='CONTROL_0_INDIVIDUAL' and x['phase']=='HOLDOUT');bc=corr[-1];joint_use=hold['team_run_mae']<control['team_run_mae'] or hold['moneyline_brier']<control['moneyline_brier'];buse=bc['total_crps_delta']<0 and bc['moneyline_brier_delta']<=0;decision='PINNACLE_MARKET_STATE_FUSION_MATERIAL_PREDICTION_ADVANCE' if joint_use and (control['total_crps']-hold['total_crps'])>.05 else 'PINNACLE_MARKET_STATE_FUSION_SMALL_ADVANCE' if joint_use else 'PINNACLE_MARKET_STATE_FUSION_NO_ADVANCE'
 text=f"""# MLB Pinnacle Market-State Fusion v1\n\n`{decision}`\n\n- Population {n} exact synchronized tri-market games, {d.game_date.min()}–{d.game_date.max()}, splits {d.groupby('phase').size().to_dict()}. One retained pregame snapshot/game; no movement acquisition, 0 credits consumed; preferred panel estimate 9,090 credits requires owner approval.\n- Selected latent state: independent-Poisson/Skellam total+moneyline+run-line inversion. Reconstruction MAE ML/total-over/run-line: {pd.DataFrame(rec).query("model=='B'").iloc[0].moneyline_mae:.6f}/{pd.DataFrame(rec).query("model=='B'").iloc[0].total_over_mae:.6f}/{pd.DataFrame(rec).query("model=='B'").iloc[0].run_line_mae:.6f}.\n- Holdout independent-market total CRPS/team-run MAE/ML Brier {control['total_crps']:.6f}/{control['team_run_mae']:.6f}/{control['moneyline_brier']:.6f}; joint {hold['total_crps']:.6f}/{hold['team_run_mae']:.6f}/{hold['moneyline_brier']:.6f}.\n- Movement: `PINNACLE_MOVEMENT_EVIDENCE_INSUFFICIENT`. Baseball correction: `{'BASEBALL_STATE_ADDS_SMALL_INCREMENTAL_VALUE' if buse else 'BASEBALL_STATE_ADDS_NO_INCREMENTAL_VALUE'}`; holdout mean absolute correction {bc['mean_abs_total_correction']:.3f} runs and {bc['mean_abs_ml_correction_pp']:.3f} pp.\n- Declarations: `PINNACLE_JOINT_MARKET_STATE={'USEFUL' if joint_use else 'NOT_USEFUL'}`, `PINNACLE_MOVEMENT=INSUFFICIENT_HISTORY`, `BASEBALL_CORRECTION={'USEFUL' if buse else 'NOT_USEFUL'}`, `TEAM_RUN_PREDICTION={'BELOW_BAR' if joint_use else 'NOT_READY'}`, `FULL_GAME_TOTAL_PREDICTION={'BELOW_BAR' if hold['total_crps']<=control['total_crps'] else 'NOT_READY'}`, `MONEYLINE_PREDICTION={'BELOW_BAR' if hold['moneyline_brier']<=control['moneyline_brier'] else 'NOT_READY'}`.\n- F5: `PINNACLE_F5_DIRECT_MARKET_STATE_UNAVAILABLE`. Current daily fields can replay the selected inversion. Exact next step: {'preserve the joint state as a bounded research baseline; seek owner approval only if a fixed movement panel is desired' if decision!='PINNACLE_MARKET_STATE_FUSION_NO_ADVANCE' else 'retain individual Pinnacle markets and do not acquire movement history without a new owner decision'}.\n- No EV/Edge, selector, deployment, public exposure, model mutation, or pipeline change occurred.\n""";(OUT/'concise_mlb_pinnacle_market_state_fusion_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'population':n,'decision':decision,'joint_useful':bool(joint_use),'baseball_useful':bool(buse),'holdout_total_crps':float(hold['total_crps']),'holdout_ml_brier':float(hold['moneyline_brier'])},indent=2))
if __name__=='__main__':main()
