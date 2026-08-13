#!/usr/bin/env python3
"""Bounded component-specific refinement of the frozen MLB PA talent prior."""
from __future__ import annotations
import hashlib,json,sys,tempfile
from pathlib import Path
import numpy as np,pandas as pd
from backend.mlb.scripts import run_mlb_pa_outcome_prediction_foundation_v1 as pa
from backend.mlb.scripts import run_mlb_multi_season_talent_prior_pa_refinement_v1 as v1

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_component_specific_talent_prior_refinement_v1/2026-08-12';SEED=20260812
GROUPS={'K':['b_k','p_k','b_whiff','p_whiff'],'BB':['b_bb','p_bb'],'HIT':['b_xba','b_xwoba','p_xwoba'],'XBH':['b_xslg','b_hard','p_hard'],'HR':['b_barrel','p_barrel']};KS=[25,50,75,125,200];TAGS=['B1','B2','B3']
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def met(y,p):
 m=pa.multi(y,p);events={'k':(y=='STRIKEOUT',p[:,0]),'bb':(y=='WALK_HBP',p[:,1]),'hit':(np.isin(y,pa.CLASSES[2:5]),p[:,2:5].sum(1)),'xbh':(np.isin(y,pa.CLASSES[3:5]),p[:,3:5].sum(1)),'hr':(y=='HOME_RUN',p[:,4]),'reach':(np.isin(y,pa.CLASSES[1:5]),p[:,1:5].sum(1))}
 for n,(yy,pp) in events.items():yy=yy.astype(float);m[n+'_brier']=np.mean((pp-yy)**2);m[n+'_log_loss']=np.mean(-yy*np.log(np.clip(pp,1e-9,1))-(1-yy)*np.log(np.clip(1-pp,1e-9,1)));m[n+'_sd']=pp.std()
 return m
def fitpred(d,x,train,q):
 m=pa.fit_model(x.loc[train,pa.B+pa.P],d.loc[train,'outcome'],'linear');return pa.pred(m,x.loc[q,pa.B+pa.P])
def main():
 OUT.mkdir(parents=True,exist_ok=True)
 # Reuse the predecessor's exact state builder without changing its artifacts.
 captured={};old=v1.OUT;v1.OUT=Path(tempfile.mkdtemp(prefix='component_prior_v1_'))
 def trace(frame,event,arg):
  if frame.f_code.co_filename==v1.__file__ and frame.f_code.co_name=='main' and event=='return':captured.update(frame.f_locals)
  return trace
 sys.settrace(trace)
 try:v1.main()
 finally:sys.settrace(None);v1.OUT=old
 d=captured['d'];frame=captured['frame'];b24=captured['b24'];p24=captured['p24'];b25=captured['b25'];p25=captured['p25'];dev=captured['dev'];val=captured['val'];hold=captured['hold'];features=pa.B+pa.P
 # Controls are exact predecessor constructions.
 xa=frame('A');xb=frame('B2',75);controls={};rows=[]
 for name,x in [('CONTROL_A',xa),('CONTROL_B_GENERIC_V1',xb)]:
  for phase,q in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:p=fitpred(d,x,dev,q);controls[name,phase]=p;rows.append({'model':name,'phase':phase,'pa':int(q.sum()),**met(d.loc[q,'outcome'].to_numpy(),p)})
 # Bounded per-component validation search.
 selected={};search=[]
 for comp,fs in GROUPS.items():
  for tag in TAGS:
   for k in KS:
    x=xb.copy();alt=frame(tag,k);x[fs]=alt[fs];p=fitpred(d,x,dev,val);mm=met(d.loc[val,'outcome'].to_numpy(),p);search.append({'component':comp,'recency':tag,'pseudo_count':k,'validation_log_loss':mm['log_loss'],'validation_brier':mm['multiclass_brier'],'component_brier':mm[comp.lower()+'_brier']})
  z=pd.DataFrame(search).query('component==@comp').sort_values(['component_brier','validation_log_loss']).iloc[0];selected[comp]=(z.recency,int(z.pseudo_count))
 xc=xb.copy()
 for comp,fs in GROUPS.items():tag,k=selected[comp];xc[fs]=frame(tag,k)[fs]
 # Compact expected-contact anchor replaces existing xBA/xwOBA state; no new model column.
 xd=xc.copy();xd['b_xba']=.55*xc.b_xba+.45*np.clip(xc.b_xwoba*.75,0,1);xd['p_xwoba']=.55*xc.p_xwoba+.45*np.clip((xc.p_ev-70)/60,0,1)
 # Compact power anchor replaces existing xSLG/barrel state using existing xSLG/hard/barrel/EV concepts.
 xe=xd.copy();xe['b_xslg']=.55*xc.b_xslg+.25*xc.b_hard+.20*np.clip(xc.b_ev/220,0,1);xe['b_barrel']=.6*xc.b_barrel+.4*np.clip((xc.b_xslg-.2)/4,0,1);xe['p_barrel']=.6*xc.p_barrel+.25*xc.p_hard+.15*np.clip((xc.p_ev-70)/100,0,1)
 models={'MODEL_C_COMPONENT_RAW':xc,'MODEL_D_EXPECTED_CONTACT':xd,'MODEL_E_EXPECTED_POWER':xe};pred={**controls}
 for name,x in models.items():
  for phase,q in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:p=fitpred(d,x,dev,q);pred[name,phase]=p;rows.append({'model':name,'phase':phase,'pa':int(q.sum()),**met(d.loc[q,'outcome'].to_numpy(),p)})
 pred['CONTROL_B_GENERIC_V1','DEVELOPMENT_DIAGNOSTIC']=fitpred(d,xb,dev,dev);pred['MODEL_E_EXPECTED_POWER','DEVELOPMENT_DIAGNOSTIC']=fitpred(d,xe,dev,dev)
 R=pd.DataFrame(rows);R.to_csv(OUT/'component_prior_model_comparison.csv',index=False);R.query("phase=='LATER_HOLDOUT'").to_csv(OUT/'component_prior_holdout_metrics.csv',index=False)
 # Contact/power comparison and stage attribution.
 cp=[];att=[]
 for phase,q in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  y=d.loc[q,'outcome'].to_numpy()
  for name in ['CONTROL_B_GENERIC_V1','MODEL_C_COMPONENT_RAW','MODEL_D_EXPECTED_CONTACT','MODEL_E_EXPECTED_POWER']:
   m=met(y,pred[name,phase]);
   for ev in ['hit','xbh','hr']:cp.append({'model':name,'phase':phase,'event':ev.upper(),'brier':m[ev+'_brier'],'log_loss':m[ev+'_log_loss'],'probability_sd':m[ev+'_sd']})
  for comp in GROUPS:att.append({'component':comp,'phase':phase,'selected_recency':selected[comp][0],'selected_pseudo_count':selected[comp][1],'generic_to_component_log_loss':met(y,pred['CONTROL_B_GENERIC_V1',phase])['log_loss']-met(y,pred['MODEL_C_COMPONENT_RAW',phase])['log_loss'],'expected_contact_increment':met(y,pred['MODEL_C_COMPONENT_RAW',phase])['log_loss']-met(y,pred['MODEL_D_EXPECTED_CONTACT',phase])['log_loss'],'expected_power_increment':met(y,pred['MODEL_D_EXPECTED_CONTACT',phase])['log_loss']-met(y,pred['MODEL_E_EXPECTED_POWER',phase])['log_loss']})
 # Diagnostic attribution only: apply the frozen selected component contract to one entity side at a time.
 xbatter=xb.copy();xstarter=xb.copy();xbatter[pa.B]=xe[pa.B];xstarter[pa.P]=xe[pa.P]
 for phase,q in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  y=d.loc[q,'outcome'].to_numpy();base=met(y,pred['CONTROL_B_GENERIC_V1',phase])
  for side,x in [('BATTER_ONLY_DIAGNOSTIC',xbatter),('STARTER_ONLY_DIAGNOSTIC',xstarter)]:
   mm=met(y,fitpred(d,x,dev,q));att.append({'component':side,'phase':phase,'selected_recency':'FROZEN_COMPONENT_CONTRACT','selected_pseudo_count':np.nan,'generic_to_component_log_loss':base['log_loss']-mm['log_loss'],'generic_to_component_brier':base['multiclass_brier']-mm['multiclass_brier'],'expected_contact_increment':np.nan,'expected_power_increment':np.nan})
 pd.DataFrame(cp).to_csv(OUT/'contact_power_probability_metrics.csv',index=False);pd.DataFrame(att).to_csv(OUT/'recency_shrinkage_attribution.csv',index=False)
 # History cohorts and temporal performance.
 cohorts=[];months=[]
 for phase,q in [('DEVELOPMENT_DIAGNOSTIC',dev),('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  sub=d.loc[q].reset_index(drop=True);bp=sub.batter_id.map(b25.pa).fillna(0)+sub.batter_id.map(b24.pa).fillna(0);sp=sub.starter_id.map(p25.bf).fillna(0)+sub.starter_id.map(p24.bf).fillna(0);bc=np.where(bp<100,'NO_VERY_LIMITED',np.where(bp<500,'DEVELOPING','ESTABLISHED'));sc=np.where(sp<200,'SPARSE',np.where(sp<700,'MODERATE','ESTABLISHED'))
  for ent,arr in ([] if phase=='DEVELOPMENT_DIAGNOSTIC' else [('BATTER',bc),('STARTER',sc)]):
   for c in np.unique(arr):
    z=arr==c
    for name in ['CONTROL_B_GENERIC_V1','MODEL_E_EXPECTED_POWER']:cohorts.append({'phase':phase,'entity':ent,'cohort':c,'model':name,'pa':int(z.sum()),**met(sub.loc[z,'outcome'].to_numpy(),pred[name,phase][z])})
  for mo in sorted(sub.game_date.str[:7].unique()):
   z=sub.game_date.str[:7].eq(mo).to_numpy()
   for name in ['CONTROL_B_GENERIC_V1','MODEL_E_EXPECTED_POWER']:months.append({'phase':phase,'month':mo,'model':name,'pa':int(z.sum()),'average_shrinkage':np.mean([selected[c][1] for c in selected])/(np.mean([selected[c][1] for c in selected])+sub.loc[z,'b_pa'].mean()),**met(sub.loc[z,'outcome'].to_numpy(),pred[name,phase][z])})
 pd.DataFrame(cohorts).to_csv(OUT/'component_prior_player_history_cohorts.csv',index=False);pd.DataFrame(months).to_csv(OUT/'component_prior_monthly_performance.csv',index=False)
 # Fixed-bin calibration and holdout separation.
 cal=[];sep=[];y=d.loc[hold,'outcome'].to_numpy()
 for name in ['CONTROL_B_GENERIC_V1','MODEL_E_EXPECTED_POWER']:
  p=pred[name,'LATER_HOLDOUT'];evs={'K':(y=='STRIKEOUT',p[:,0]),'BB_HBP':(y=='WALK_HBP',p[:,1]),'HIT':(np.isin(y,pa.CLASSES[2:5]),p[:,2:5].sum(1)),'HR':(y=='HOME_RUN',p[:,4])}
  for ev,(yy,pp) in evs.items():
   if name=='MODEL_E_EXPECTED_POWER':sep.append({'event':ev,'mean':pp.mean(),'sd':pp.std(),'p05':np.quantile(pp,.05),'p25':np.quantile(pp,.25),'median':np.median(pp),'p75':np.quantile(pp,.75),'p95':np.quantile(pp,.95),'minimum':pp.min(),'maximum':pp.max()})
   for lo,hi in zip([0,.05,.1,.2,.3,.4,.6],[.05,.1,.2,.3,.4,.6,1.01]):
    z=(pp>=lo)&(pp<hi)
    if z.any():cal.append({'model':name,'event':ev,'bin':f'{lo:.2f}-{hi:.2f}','pa':int(z.sum()),'predicted':pp[z].mean(),'observed':yy[z].mean(),'gap':pp[z].mean()-yy[z].mean(),'brier':np.mean((pp[z]-yy[z])**2)})
 pd.DataFrame(cal).to_csv(OUT/'component_prior_calibration.csv',index=False);pd.DataFrame(sep).to_csv(OUT/'component_prior_probability_separation.csv',index=False)
 # State stability, representative limited/established classes combined.
 stab=[]
 for ent,idc,fs in [('BATTER','batter_id',list(v1.BF)),('STARTER','starter_id',list(v1.PF))]:
  hist=d[idc].map((b24.pa+b25.pa) if ent=='BATTER' else (p24.bf+p25.bf)).fillna(0)
  for cohort,z0 in [('LIMITED',hist<200),('ESTABLISHED',hist>=500)]:
   for name,x in [('CONTROL_B_GENERIC_V1',xb),('MODEL_E_EXPECTED_POWER',xe)]:
    z=pd.concat([d.loc[z0,[idc,'game_date']],x.loc[z0,fs]],axis=1).drop_duplicates([idc,'game_date']).sort_values([idc,'game_date']);mv=z.groupby(idc)[fs].diff().abs().mean(axis=1);stab.append({'entity':ent,'cohort':cohort,'model':name,'player_days':len(z),'mean_absolute_change':mv.mean(),'p95_change':mv.quantile(.95),'large_jump_frequency':(mv>.05).mean()})
 pd.DataFrame(stab).to_csv(OUT/'component_prior_state_stability.csv',index=False)
 # Contracts and final gate.
 d[['game_pk','game_date','batter_id','starter_id','outcome','split']].to_csv(OUT/'component_prior_population_manifest.csv',index=False);pd.DataFrame([{'component':c,'selected_recency':selected[c][0],**{f'w_{y}':v1.REC[selected[c][0]][y] for y in [2024,2025,2026]}} for c in GROUPS]).to_csv(OUT/'component_recency_contract.csv',index=False);pd.DataFrame([{'component':c,'pseudo_count':selected[c][1],'target':'feature-specific population constant','selection':'validation component Brier from fixed ladder'} for c in GROUPS]).to_csv(OUT/'component_shrinkage_contract.csv',index=False);pd.DataFrame([{'entity':'BATTER','replacement':'b_xba = .55 xBA + .45 (.75 xwOBA)','inputs':'xBA|xwOBA'},{'entity':'STARTER','replacement':'p_xwoba = .55 xwOBA + .45 scaled EV','inputs':'xwOBA|EV allowed'}]).to_csv(OUT/'expected_contact_prior_contract.csv',index=False);pd.DataFrame([{'entity':'BATTER','replacement':'compact xSLG/hard/barrel/EV anchor','inputs':'xSLG|hard|barrel|EV'},{'entity':'STARTER','replacement':'compact barrel/hard/EV anchor','inputs':'barrel allowed|hard allowed|EV allowed'}]).to_csv(OUT/'expected_power_prior_contract.csv',index=False)
 contract={'taxonomy':pa.CLASSES,'architecture':'frozen L2 multinomial C=.5, imputer, scaler, batter+starter features','temporal_split':d.groupby('split').size().to_dict(),'selected':selected,'challenger_ladder':['CONTROL_A','CONTROL_B_GENERIC_V1','MODEL_C_COMPONENT_RAW','MODEL_D_EXPECTED_CONTACT','MODEL_E_EXPECTED_POWER'],'holdout_opened_once':'YES'};(OUT/'component_prior_contract.json').write_text(json.dumps(contract,indent=2)+'\n')
 b=R.query("model=='CONTROL_B_GENERIC_V1' and phase=='LATER_HOLDOUT'").iloc[0];e=R.query("model=='MODEL_E_EXPECTED_POWER' and phase=='LATER_HOLDOUT'").iloc[0];dll=b.log_loss-e.log_loss;dbr=b.multiclass_brier-e.multiclass_brier;hit=b.hit_brier-e.hit_brier;xbh=b.xbh_brier-e.xbh_brier;hr=b.hr_brier-e.hr_brier;meaning=dll>=.002 and dbr>=.001 and hit>0 and (xbh>0 or hr>0);decision='COMPONENT_SPECIFIC_TALENT_PRIOR_MATERIAL_PREDICTIVE_ADVANCE' if meaning else 'COMPONENT_SPECIFIC_TALENT_PRIOR_SMALL_IMPROVEMENT' if dll>0 and dbr>0 else 'COMPONENT_SPECIFIC_TALENT_PRIOR_NO_IMPROVEMENT';gate='READY' if meaning else 'NOT_READY';use=lambda x:'USEFUL' if x>0 else 'NOT_USEFUL'
 pd.DataFrame([{'generic_log_loss':b.log_loss,'component_log_loss':e.log_loss,'log_loss_improvement':dll,'relative_improvement_pct':100*dll/b.log_loss,'generic_brier':b.multiclass_brier,'component_brier':e.multiclass_brier,'brier_improvement':dbr,'k_brier_improvement':b.k_brier-e.k_brier,'bb_brier_improvement':b.bb_brier-e.bb_brier,'hit_brier_improvement':hit,'xbh_brier_improvement':xbh,'hr_brier_improvement':hr,'classification':'MEANINGFUL' if meaning else 'SMALL' if decision.endswith('IMPROVEMENT') else 'NEGLIGIBLE','decision':decision,'game_level_propagation':gate}]).to_csv(OUT/'component_prior_materiality.csv',index=False)
 text=f"""# MLB Component-Specific Talent Prior Refinement v1\n\n`{decision}`\n\n- Population: {len(d):,} PAs / {d.game_pk.nunique():,} games; frozen splits {d.groupby('split').size().to_dict()}.\n- Selected contracts: {selected}. Generic holdout log loss/Brier {b.log_loss:.6f}/{b.multiclass_brier:.6f}; component model {e.log_loss:.6f}/{e.multiclass_brier:.6f}; improvement {dll:+.6f}/{dbr:+.6f}.\n- Holdout class Brier improvements: K {b.k_brier-e.k_brier:+.6f}, BB/HBP {b.bb_brier-e.bb_brier:+.6f}, hit {hit:+.6f}, XBH {xbh:+.6f}, HR {hr:+.6f}.\n- `K_PRIOR = {use(b.k_brier-e.k_brier)}`; `BB_PRIOR = {use(b.bb_brier-e.bb_brier)}`; `HIT_PRIOR = {use(hit)}`; `POWER_PRIOR = {use(max(xbh,hr))}`; `GAME_LEVEL_PROPAGATION = {gate}`.\n- Exact next step: {'one bounded scoring propagation experiment' if gate=='READY' else 'retain the generic V1 prior and stop component-specific propagation'}.\n""";(OUT/'concise_mlb_component_specific_talent_prior_refinement_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files));print(json.dumps({'selected':selected,'generic_log_loss':b.log_loss,'component_log_loss':e.log_loss,'generic_brier':b.multiclass_brier,'component_brier':e.multiclass_brier,'decision':decision,'gate':gate},indent=2))
if __name__=='__main__':main()
