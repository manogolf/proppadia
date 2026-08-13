#!/usr/bin/env python3
"""Analyze authorized Pinnacle four-snapshot movement panel."""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
from sklearn.linear_model import Ridge,LogisticRegression
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_movement_and_later_market_prediction_v1/2026-08-12';FUS=ROOT/'artifacts/analysis/model_development/mlb_pinnacle_market_state_fusion_v1/2026-08-12/pinnacle_fusion_population.csv';ACQ=OUT/'movement_historical_acquisition_manifest.csv';TARGET={'A':18,'B':8,'C':4,'D':1}
def sh(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def ap(x):x=float(x);return 100/(x+100) if x>0 else -x/(-x+100)
def ece(p,y):
 v=0
 for lo,hi in [(0,.2),(.2,.4),(.4,.6),(.6,.8),(.8,1.01)]:
  q=(p>=lo)&(p<hi)
  if q.any():v+=q.mean()*abs(p[q].mean()-y[q].mean())
 return v
def parse_payload(path,game):
 j=json.loads((ROOT/path).read_text());events=j.get('data',[]);cand=[e for e in events if e.get('id')==game.event_id]
 if len(cand)!=1:return None
 e=cand[0];books=[b for b in e.get('bookmakers',[]) if b.get('key')=='pinnacle']
 if len(books)!=1:return None
 markets={m['key']:m for m in books[0].get('markets',[])}
 try:
  h2={o['name']:o for o in markets['h2h']['outcomes']};tot=markets['totals']['outcomes'];spr=markets['spreads']['outcomes'];over=next(o for o in tot if o['name']=='Over');under=next(o for o in tot if o['name']=='Under');hs=next(o for o in spr if o['name']==game.home_team);aws=next(o for o in spr if o['name']==game.away_team);hp,apx=ap(h2[game.home_team]['price']),ap(h2[game.away_team]['price']);op,up=ap(over['price']),ap(under['price']);hr,ar=ap(hs['price']),ap(aws['price'])
 except (KeyError,StopIteration):return None
 return {'home_ml_price':h2[game.home_team]['price'],'away_ml_price':h2[game.away_team]['price'],'home_ml_nv':hp/(hp+apx),'total_line':over['point'],'over_price':over['price'],'under_price':under['price'],'over_nv':op/(op+up),'home_spread':hs['point'],'away_spread':aws['point'],'home_rl_price':hs['price'],'away_rl_price':aws['price'],'home_rl_nv':hr/(hr+ar)}
def metrics(y,p):
 y=np.asarray(y,float);p=np.clip(np.asarray(p,float),1e-8,1-1e-8);return {'brier':np.mean((p-y)**2),'log_loss':np.mean(-y*np.log(p)-(1-y)*np.log(1-p)),'ece':ece(p,y),'accuracy':np.mean((p>=.5)==y),'probability_sd':p.std()}
def main():
 base=pd.read_csv(FUS);base['event_id']=pd.read_csv(ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/moneyline_pinnacle_join.csv').set_index('game_pk').loc[base.game_pk,'event_id'].to_numpy();base['home_team']=pd.read_csv(ROOT/'artifacts/analysis/model_development/mlb_run_line_prediction_foundation_v1/2026-08-10/authoritative_run_line_market_population.csv').set_index('game_pk').loc[base.game_pk,'home_team'].to_numpy();base['away_team']=pd.read_csv(ROOT/'artifacts/analysis/model_development/mlb_run_line_prediction_foundation_v1/2026-08-10/authoritative_run_line_market_population.csv').set_index('game_pk').loc[base.game_pk,'away_team'].to_numpy();acq=pd.read_csv(ACQ);old=pd.read_csv(ROOT/'artifacts/analysis/model_development/mlb_pinnacle_incremental_information_benchmark_v1/2026-08-10/pinnacle_historical_snapshot_manifest.csv');raw=[]
 for g in base.itertuples():
  start=pd.Timestamp(g.scheduled_start_utc);day=acq[acq.game_date.eq(g.game_date)]
  sources={x.slot.split('_')[0]:x for x in day.itertuples()}
  om=old[old.game_date.eq(g.game_date)]
  if len(om):sources['D']=om.iloc[0]
  for slot,h in TARGET.items():
   src=sources.get(slot);target=start-pd.Timedelta(hours=h)
   if src is None:continue
   stamp=pd.Timestamp(src.returned_snapshot_utc if slot!='D' else src.provider_snapshot_utc);err=(target-stamp).total_seconds()/60
   if stamp>target or err>120 or stamp>=start:continue
   vals=parse_payload(src.raw_path,g)
   if vals:raw.append({'game_pk':g.game_pk,'game_date':g.game_date,'slot':slot,'target_lead_hours':h,'target_utc':target,'snapshot_utc':stamp,'actual_lead_hours':(start-stamp).total_seconds()/3600,'backward_error_minutes':err,'raw_path':src.raw_path,**vals})
 panel=pd.DataFrame(raw);acq.to_csv(OUT/'movement_raw_snapshot_manifest.csv',index=False);wide=panel.pivot(index='game_pk',columns='slot');cov=[]
 for g in base.itertuples():
  z=panel[panel.game_pk.eq(g.game_pk)];slots=set(z.slot);cov.append({'game_pk':g.game_pk,'game_date':g.game_date,'A':int('A'in slots),'B':int('B'in slots),'C':int('C'in slots),'D':int('D'in slots),'snapshot_count':len(slots),'primary_A_C_D':{'A','C','D'}.issubset(slots)})
 cv=pd.DataFrame(cov);cv.to_csv(OUT/'movement_panel_coverage.csv',index=False);panel.to_csv(OUT/'movement_market_normalization.csv',index=False);ids=cv[cv.primary_A_C_D].game_pk;d=base[base.game_pk.isin(ids)].copy().sort_values(['game_date','game_pk']).reset_index(drop=True);assert len(d)>0
 for slot in TARGET:
  z=panel[panel.slot.eq(slot)].set_index('game_pk');
  for c in ['home_ml_nv','total_line','over_nv','home_spread','home_rl_nv','actual_lead_hours']:d[slot+'_'+c]=d.game_pk.map(z[c])
 n=len(d);i1=int(n*.6);i2=int(n*.8);d['movement_phase']=np.where(np.arange(n)<i1,'DEVELOPMENT',np.where(np.arange(n)<i2,'VALIDATION','HOLDOUT'))
 # Fixed movement features.
 for a,b in [('A','B'),('B','C'),('C','D'),('A','D')]:
  for c in ['home_ml_nv','total_line','over_nv','home_spread','home_rl_nv']:d[f'{a}_{b}_{c}_change']=d[f'{b}_{c}']-d[f'{a}_{c}']
 d['A_D_ml_abs']=d.A_D_home_ml_nv_change.abs();d['A_D_total_abs']=d.A_D_total_line_change.abs();d['A_D_rl_abs']=d.A_D_home_rl_nv_change.abs();d['ml_flip']=(d.A_home_ml_nv>=.5)!=(d.D_home_ml_nv>=.5);d['rl_flip']=(d.A_home_rl_nv>=.5)!=(d.D_home_rl_nv>=.5);d['total_crossings']=(d[['A_total_line','B_total_line','C_total_line','D_total_line']].diff(axis=1).abs()>0).sum(axis=1);d['ml_rate_hour']=d.A_D_home_ml_nv_change/(d.A_actual_lead_hours-d.D_actual_lead_hours);d['total_rate_hour']=d.A_D_total_line_change/(d.A_actual_lead_hours-d.D_actual_lead_hours);d['rl_rate_hour']=d.A_D_home_rl_nv_change/(d.A_actual_lead_hours-d.D_actual_lead_hours);d.to_csv(OUT/'movement_feature_population.csv',index=False)
 mcols=['A_home_ml_nv','A_total_line','A_home_rl_nv','A_B_home_ml_nv_change','B_C_home_ml_nv_change','A_B_total_line_change','B_C_total_line_change','A_B_home_rl_nv_change','B_C_home_rl_nv_change','A_actual_lead_hours','C_actual_lead_hours'];bcols=['home_wp','away_wp','home_rs','home_ra','away_rs','away_ra','home_rest','away_rest','home_win_probability','expected_total'];tr=d.movement_phase.eq('DEVELOPMENT');results={'ml':[],'total':[],'rl':[]};pred={}
 def regress(target,base0,base1,name,extra=[]):
  for phase in ['VALIDATION','HOLDOUT']:
   q=d.movement_phase.eq(phase);models={'A_PERSISTENCE':d[base0].to_numpy(),'C_PERSISTENCE':d[base1].to_numpy()}
   for lab,cols in [('MOVEMENT',mcols),('MOVEMENT_BASEBALL',mcols+bcols)]:models[lab]=make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),Ridge(alpha=10)).fit(d.loc[tr,cols],d.loc[tr,target]).predict(d[cols])
   for lab,x in models.items():
    e=x[q]-d.loc[q,target];results[name].append({'model':lab,'phase':phase,'games':q.sum(),'mae':np.mean(abs(e)),'rmse':np.sqrt(np.mean(e**2)),'bias':np.mean(e),'exact_rate':np.mean(abs(e)<1e-9),'within_0_5_rate':np.mean(abs(e)<=.5)});pred[name,lab,phase]=x[q]
 regress('D_home_ml_nv','A_home_ml_nv','C_home_ml_nv','ml');regress('D_total_line','A_total_line','C_total_line','total');regress('D_home_rl_nv','A_home_rl_nv','C_home_rl_nv','rl')
 pd.DataFrame(results['ml']).to_csv(OUT/'moneyline_later_market_prediction.csv',index=False);pd.DataFrame(results['total']).to_csv(OUT/'total_later_market_prediction.csv',index=False);pd.DataFrame(results['rl']).to_csv(OUT/'runline_later_market_prediction.csv',index=False)
 inc=[]
 for market in results:
  x=pd.DataFrame(results[market]);
  for phase in ['VALIDATION','HOLDOUT']:
   a=x.query("phase==@phase and model=='MOVEMENT'").iloc[0];b=x.query("phase==@phase and model=='MOVEMENT_BASEBALL'").iloc[0];inc.append({'market':market,'phase':phase,'mae_delta_baseball_minus_movement':b.mae-a.mae,'baseball_improves':b.mae<a.mae})
 pd.DataFrame(inc).to_csv(OUT/'baseball_state_later_market_increment.csv',index=False)
 # Descriptive movement.
 desc=[]
 for month,g in d.groupby(d.game_date.str[:7]):
  for market,col,flip in [('MONEYLINE','A_D_ml_abs','ml_flip'),('TOTAL','A_D_total_abs',None),('RUNLINE','A_D_rl_abs','rl_flip')]:desc.append({'month':month,'market':market,'games':len(g),'mean_abs_A_D':g[col].mean(),'median_abs_A_D':g[col].median(),'p90_abs_A_D':g[col].quantile(.9),'flip_rate':g[flip].mean() if flip else np.nan,'zero_rate':np.mean(g[col]==0),'half_rate':np.mean(g[col]==.5) if market=='TOTAL' else np.nan,'one_rate':np.mean(g[col]==1) if market=='TOTAL' else np.nan,'gt_one_rate':np.mean(g[col]>1) if market=='TOTAL' else np.nan})
 pd.DataFrame(desc).to_csv(OUT/'movement_descriptive_statistics.csv',index=False)
 # Outcome: D state vs D+path development fit.
 om=[];tm=[];team=[]
 for phase in ['VALIDATION','HOLDOUT']:
  q=d.movement_phase.eq(phase);X=d[mcols].copy();log=make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),LogisticRegression(C=.05,max_iter=2000)).fit(X[tr],d.loc[tr,'winner_home']);pp=log.predict_proba(X)[:,1]
  for lab,p in [('A_STATE',d.A_home_ml_nv),('C_STATE',d.C_home_ml_nv),('D_STATE',d.D_home_ml_nv),('D_PLUS_PATH',pp)]:om.append({'model':lab,'phase':phase,'games':q.sum(),**metrics(d.loc[q,'winner_home'],p[q])})
  ridge=make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),Ridge(alpha=10)).fit(X[tr],d.loc[tr,'final_total']-d.loc[tr,'D_total_line']);adj=ridge.predict(X);states={'A_STATE':d.A_total_line,'C_STATE':d.C_total_line,'D_STATE':d.D_total_line,'D_PLUS_PATH':d.D_total_line+adj}
  for lab,x in states.items():tm.append({'model':lab,'phase':phase,'games':q.sum(),'total_mae':np.mean(abs(x[q]-d.loc[q,'final_total'])),'bias':np.mean(x[q]-d.loc[q,'final_total'])})
  for lab,prefix in [('A_STATE','A'),('C_STATE','C'),('D_STATE','D')]:
   share=np.clip(d[f'{prefix}_home_ml_nv'],.2,.8);hm=d[f'{prefix}_total_line']*share;am=d[f'{prefix}_total_line']-hm;team.append({'model':lab,'phase':phase,'home_mae':np.mean(abs(hm[q]-d.loc[q,'final_home_runs'])),'away_mae':np.mean(abs(am[q]-d.loc[q,'final_away_runs'])),'margin_mae':np.mean(abs((hm[q]-am[q])-(d.loc[q,'final_home_runs']-d.loc[q,'final_away_runs'])))})
 pd.DataFrame(om).to_csv(OUT/'moneyline_outcome_movement_metrics.csv',index=False);pd.DataFrame(tm).to_csv(OUT/'total_outcome_movement_metrics.csv',index=False);pd.DataFrame(team).to_csv(OUT/'team_run_margin_movement_metrics.csv',index=False)
 # Regimes/reversals/speed/cross-market/stability.
 reg=[]
 d['ml_regime']=np.where(d.A_D_home_ml_nv_change>=.02,'HOME_STRENGTHENS',np.where(d.A_D_home_ml_nv_change<=-.02,'HOME_WEAKENS','STABLE'));d['total_regime']=np.where(d.A_D_total_line_change>=.5,'RISES',np.where(d.A_D_total_line_change<=-.5,'FALLS','UNCHANGED'));d['rl_regime']=np.where(d.A_D_home_rl_nv_change>=.02,'HOME_STRENGTHENS',np.where(d.A_D_home_rl_nv_change<=-.02,'HOME_WEAKENS','STABLE'))
 for market,col in [('MONEYLINE','ml_regime'),('TOTAL','total_regime'),('RUNLINE','rl_regime')]:
  for (phase,rg),g in d.groupby(['movement_phase',col]):reg.append({'market':market,'phase':phase,'regime':rg,'games':len(g),'home_win_rate':g.winner_home.mean(),'mean_total':g.final_total.mean(),'mean_A_D_movement':g[{'MONEYLINE':'A_D_home_ml_nv_change','TOTAL':'A_D_total_line_change','RUNLINE':'A_D_home_rl_nv_change'}[market]].mean()})
 pd.DataFrame(reg).to_csv(OUT/'movement_direction_regimes.csv',index=False);revs=[]
 for market,ab,cd in [('MONEYLINE','A_B_home_ml_nv_change','C_D_home_ml_nv_change'),('TOTAL','A_B_total_line_change','C_D_total_line_change'),('RUNLINE','A_B_home_rl_nv_change','C_D_home_rl_nv_change')]:
  rev=np.sign(d[ab])*np.sign(d[cd])<0
  for phase in ['VALIDATION','HOLDOUT']:
   q=d.movement_phase.eq(phase)&rev;revs.append({'market':market,'phase':phase,'reversals':q.sum(),'frequency':q.sum()/max((d.movement_phase==phase).sum(),1),'mean_magnitude':(d.loc[q,ab].abs()+d.loc[q,cd].abs()).mean(),'home_win_rate':d.loc[q,'winner_home'].mean()})
 pd.DataFrame(revs).to_csv(OUT/'movement_reversal_analysis.csv',index=False);speed=[]
 for market,col in [('MONEYLINE','ml_rate_hour'),('TOTAL','total_rate_hour'),('RUNLINE','rl_rate_hour')]:
  cuts=d.loc[tr,col].abs().quantile([1/3,2/3]).to_list();band=pd.cut(d[col].abs(),[-np.inf,cuts[0],cuts[1],np.inf],labels=['SLOW','MODERATE','FAST'])
  for (phase,b),g in d.groupby(['movement_phase',band],observed=True):speed.append({'market':market,'phase':phase,'speed_band':b,'games':len(g),'mean_abs_rate':g[col].abs().mean(),'home_win_rate':g.winner_home.mean(),'mean_total':g.final_total.mean()})
 pd.DataFrame(speed).to_csv(OUT/'movement_speed_analysis.csv',index=False);cross=[]
 for target,source in [('D_home_rl_nv','A_D_home_ml_nv_change'),('D_home_ml_nv','A_D_total_line_change'),('D_home_ml_nv','A_D_home_rl_nv_change')]:
  for phase in ['VALIDATION','HOLDOUT']:
   q=d.movement_phase.eq(phase);r=Ridge(alpha=10).fit(d.loc[tr,[source]],d.loc[tr,target]);pr=r.predict(d[[source]]);cross.append({'target':target,'source':source,'phase':phase,'mae':np.mean(abs(pr[q]-d.loc[q,target])),'correlation':d.loc[q,[source,target]].corr().iloc[0,1]})
 pd.DataFrame(cross).to_csv(OUT/'cross_market_movement.csv',index=False)
 stable=[]
 for phase in ['VALIDATION','HOLDOUT']:
  for month,g in d[d.movement_phase.eq(phase)].groupby(d.game_date.str[:7]):
   q=g.index;stable.append({'phase':phase,'month':month,'games':len(g),'ml_C_persistence_mae':np.mean(abs(g.C_home_ml_nv-g.D_home_ml_nv)),'ml_movement_mae':np.mean(abs(pred['ml','MOVEMENT',phase][d[d.movement_phase.eq(phase)].index.get_indexer(q)]-g.D_home_ml_nv)),'total_C_persistence_mae':np.mean(abs(g.C_total_line-g.D_total_line)),'total_movement_mae':np.mean(abs(pred['total','MOVEMENT',phase][d[d.movement_phase.eq(phase)].index.get_indexer(q)]-g.D_total_line))})
 pd.DataFrame(stable).to_csv(OUT/'movement_temporal_stability.csv',index=False)
 contract={'targets_hours':TARGET,'maximum_backward_tolerance_minutes':120,'selection':'nearest provider snapshot at or before target; missing otherwise','primary_population':'A+C+D required; B optional','features':mcols,'raw_responses_immutable':True,'credits_consumed':int(acq.request_cost.sum()),'quota_start':82198,'quota_end':75088};(OUT/'movement_feature_contract.json').write_text(json.dumps(contract,indent=2)+'\n')
 (OUT/'current_capture_movement_compatibility.md').write_text('# Current capture movement compatibility\n\n`CURRENT_CAPTURE_SUPPORTS_MOVEMENT_MODEL = PARTIAL`\n\nThe existing early/future-slate and 05:30, 08:30, 11:00, 13:00, 16:30 PT windows can generate ordered movement features without scheduler changes. They do not guarantee exact T-18/T-8/T-4/T-1 observations for every first-pitch time, so deterministic slot matching/fallback remains partial.\n')
 mlr=pd.DataFrame(results['ml']);tor=pd.DataFrame(results['total']);mh=mlr.query("phase=='HOLDOUT'").set_index('model');th=tor.query("phase=='HOLDOUT'").set_index('model');mlimp=mh.loc['C_PERSISTENCE'].mae-mh.loc['MOVEMENT'].mae;ti=th.loc['C_PERSISTENCE'].mae-th.loc['MOVEMENT'].mae;mo=pd.DataFrame(om).query("phase=='HOLDOUT'").set_index('model');toh=pd.DataFrame(tm).query("phase=='HOLDOUT'").set_index('model');outimp=mo.loc['D_STATE'].brier-mo.loc['D_PLUS_PATH'].brier;totimp=toh.loc['D_STATE'].total_mae-toh.loc['D_PLUS_PATH'].total_mae;later='PINNACLE_MOVEMENT_MATERIAL_LATER_MARKET_SIGNAL' if mlimp>.005 or ti>.1 else 'PINNACLE_MOVEMENT_SMALL_LATER_MARKET_SIGNAL' if mlimp>0 or ti>0 else 'PINNACLE_MOVEMENT_DOES_NOT_PREDICT_LATER_MARKET';outdec='PINNACLE_MOVEMENT_MATERIAL_INCREMENTAL_OUTCOME_SIGNAL' if outimp>.005 and totimp>.1 else 'PINNACLE_MOVEMENT_SMALL_INCREMENTAL_OUTCOME_SIGNAL' if outimp>0 and totimp>0 else 'PINNACLE_MOVEMENT_NO_INCREMENTAL_OUTCOME_SIGNAL';bi=pd.DataFrame(inc);bdec='YES' if bi.query("phase=='HOLDOUT'").baseball_improves.all() else 'MIXED' if bi.query("phase=='HOLDOUT'").baseball_improves.any() else 'NO'
 desc=pd.DataFrame(desc);text=f"""# MLB Pinnacle Movement and Later-Market Prediction v1\n\n- Exact base population 764 games; primary A+C+D movement population {n}. Coverage: {cv.snapshot_count.value_counts().sort_index().to_dict()}. Median lead A/B/C/D: {panel.groupby('slot').actual_lead_hours.median().round(3).to_dict()} hours.\n- Acquisition: 237/237 requests succeeded, 7,110 credits; quota 82,198 → 75,088.\n- A→D mean absolute movement ML/total/RL: {d.A_D_ml_abs.mean():.4f} probability / {d.A_D_total_abs.mean():.3f} runs / {d.A_D_rl_abs.mean():.4f} probability.\n- Moneyline later-market holdout: C persistence MAE {mh.loc['C_PERSISTENCE'].mae:.6f}; movement {mh.loc['MOVEMENT'].mae:.6f}. Total line: C persistence {th.loc['C_PERSISTENCE'].mae:.6f}; movement {th.loc['MOVEMENT'].mae:.6f}. `BASEBALL_STATE_PREDICTS_LATER_PINNACLE = {bdec}`.\n- Outcome holdout: D moneyline Brier {mo.loc['D_STATE'].brier:.6f}; D+path {mo.loc['D_PLUS_PATH'].brier:.6f}. D total MAE {toh.loc['D_STATE'].total_mae:.6f}; D+path {toh.loc['D_PLUS_PATH'].total_mae:.6f}. Reversal, speed, cross-market, and temporal results are reported in dedicated artifacts.\n- `{later}`; `{outdec}`; `CURRENT_CAPTURE_SUPPORTS_MOVEMENT_MODEL = PARTIAL`.\n- Exact next step: {'preserve the movement model as research-only and run one prospective later-market forecast audit using current capture windows' if later!='PINNACLE_MOVEMENT_DOES_NOT_PREDICT_LATER_MARKET' else 'retain snapshot D as the market state and do not operationalize movement features'}. No EV/Edge/ROI, selector, deployment, public exposure, model mutation, or scheduler change occurred.\n""";(OUT/'concise_mlb_pinnacle_movement_and_later_market_prediction_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256' and x.name!='movement_historical_acquisition_manifest.csv');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in [OUT/'movement_historical_acquisition_manifest.csv']+files));print(json.dumps({'primary_population':n,'later_market':later,'outcome':outdec,'baseball':bdec,'ml_persistence_mae':mh.loc['C_PERSISTENCE'].mae,'ml_movement_mae':mh.loc['MOVEMENT'].mae,'total_persistence_mae':th.loc['C_PERSISTENCE'].mae,'total_movement_mae':th.loc['MOVEMENT'].mae},indent=2))
if __name__=='__main__':main()
