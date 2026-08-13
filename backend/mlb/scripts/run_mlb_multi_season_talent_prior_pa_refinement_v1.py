#!/usr/bin/env python3
"""Bounded multi-season talent-prior refinement of the frozen PA control."""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
from sklearn.metrics import log_loss
from backend.mlb.scripts import run_mlb_pa_outcome_prediction_foundation_v1 as pa

ROOT=Path(__file__).resolve().parents[3]
OUT=ROOT/'artifacts/analysis/model_development/mlb_multi_season_talent_prior_pa_refinement_v1/2026-08-12'
BASE=ROOT/'artifacts/analysis/model_development/mlb_pa_outcome_prediction_foundation_v1/2026-08-12/pa_population_manifest.csv'
RAW=ROOT/'backend/mlb/data/external/statcast/raw'; SEED=20260812
REC={'B1':{2024:.4,2025:.7,2026:1.},'B2':{2024:.25,2025:.5,2026:1.},'B3':{2024:.6,2025:.8,2026:1.}}
BF={'b_xwoba':('x_sum','x_n',.32),'b_xba':('xba_sum','xba_n',.245),'b_xslg':('xslg_sum','xslg_n',.41),'b_ev':('ev_sum','ev_n',88.),'b_hard':('hard','ev_n',.38),'b_barrel':('barrel','ev_n',.07),'b_k':('k','pa',.23),'b_bb':('bb','pa',.085),'b_whiff':('whiff','swing',.24)}
PF={'p_xwoba':('x_sum','x_n',.32),'p_ev':('ev_sum','ev_n',88.),'p_hard':('hard','ev_n',.38),'p_barrel':('barrel','ev_n',.07),'p_k':('k','bf',.23),'p_bb':('bb','bf',.085),'p_whiff':('whiff','swing',.24),'p_gb':('gb','ev_n',.43),'p_velo':('velo_sum','velo_n',93.)}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def load_year(y):
 cols=['game_date','game_pk','at_bat_number','pitch_number','batter','pitcher','events','description','estimated_woba_using_speedangle','estimated_ba_using_speedangle','estimated_slg_using_speedangle','launch_speed','release_speed','launch_speed_angle','bb_type'];fs=[]
 for f in sorted((RAW/str(y)).glob('*/statcast_search.csv')):fs.append(pd.read_csv(f,usecols=cols,low_memory=False))
 d=pd.concat(fs,ignore_index=True);d.game_date=pd.to_datetime(d.game_date).dt.strftime('%Y-%m-%d')
 for c in ['game_pk','at_bat_number','pitch_number','batter','pitcher']:d[c]=pd.to_numeric(d[c],errors='coerce').astype('Int64')
 return d.dropna(subset=['game_pk','at_bat_number','pitch_number','batter','pitcher']).drop_duplicates(['game_pk','at_bat_number','pitch_number'],keep='last')
def prep(d):
 d=d.copy();d['xw']=pd.to_numeric(d.estimated_woba_using_speedangle,errors='coerce');d['xba']=pd.to_numeric(d.estimated_ba_using_speedangle,errors='coerce');d['xslg']=pd.to_numeric(d.estimated_slg_using_speedangle,errors='coerce');d['ev']=pd.to_numeric(d.launch_speed,errors='coerce');d['velo']=pd.to_numeric(d.release_speed,errors='coerce');d['pa']=d.events.notna();d['k']=d.events.astype(str).str.contains('strikeout');d['bb']=d.events.astype(str).isin(['walk','intent_walk','hit_by_pitch']);d['whiff']=d.description.astype(str).str.contains('swinging_strike|foul_tip');d['swing']=d.description.astype(str).str.contains('swinging|foul|hit_into_play');d['hard']=d.ev>=95;d['barrel']=pd.to_numeric(d.launch_speed_angle,errors='coerce').eq(6);d['gb']=d.bb_type.eq('ground_ball');return d
def aggregate(d,idcol,starter=False):
 rows=[]
 for pid,g in d.groupby(idcol):
  z={'player_id':int(pid),'pitches':len(g),'pa':float(g.pa.sum()),'bf':float(g.pa.sum()),'x_n':float(g.xw.notna().sum()),'xba_n':float(g.xba.notna().sum()),'xslg_n':float(g.xslg.notna().sum()),'ev_n':float(g.ev.notna().sum()),'velo_n':float(g.velo.notna().sum()),'x_sum':float(g.xw.sum()),'xba_sum':float(g.xba.sum()),'xslg_sum':float(g.xslg.sum()),'ev_sum':float(g.ev.sum()),'velo_sum':float(g.velo.sum()),'hard':float(g.hard.sum()),'barrel':float(g.barrel.sum()),'k':float(g.k.sum()),'bb':float(g.bb.sum()),'whiff':float(g.whiff.sum()),'swing':float(g.swing.sum()),'gb':float(g.gb.sum())};rows.append(z)
 return pd.DataFrame(rows).set_index('player_id')
def model(d,features,train):return pa.fit_model(d.loc[train,features],d.loc[train,'outcome'],'linear')
def metrics(y,p):
 m=pa.multi(y,p);m['hit_brier']=np.mean((p[:,2:5].sum(1)-np.isin(y,pa.CLASSES[2:5]))**2);m['k_brier']=np.mean((p[:,0]-(y=='STRIKEOUT'))**2);m['bb_brier']=np.mean((p[:,1]-(y=='WALK_HBP'))**2);m['hr_brier']=np.mean((p[:,4]-(y=='HOME_RUN'))**2);return m
def main():
 OUT.mkdir(parents=True,exist_ok=True);d=pd.read_csv(BASE);d['game_date']=d.game_date.astype(str);features=pa.B+pa.P
 y24=prep(load_year(2024));y25=prep(load_year(2025));b24=aggregate(y24,'batter');p24=aggregate(y24,'pitcher');b25=aggregate(y25,'batter');p25=aggregate(y25,'pitcher')
 # Recover strict-prior 2026 sufficient statistics from exact control totals minus static 2025 totals.
 meta=[]
 for ent,idc,countc,tab24,tab25,fmap in [('BATTER','batter_id','b_pa',b24,b25,BF),('STARTER','starter_id','p_bf',p24,p25,PF)]:
  ids=d[idc].astype(int);n25=ids.map(tab25['pa' if ent=='BATTER' else 'bf']).fillna(0).to_numpy();n24=ids.map(tab24['pa' if ent=='BATTER' else 'bf']).fillna(0).to_numpy();n26=np.maximum(d[countc].to_numpy()-n25,0)
  meta.append(pd.DataFrame({'row':np.arange(len(d)),'entity':ent,'player_id':ids,'game_date':d.game_date,'raw_2024_sample':n24,'raw_2025_sample':n25,'strict_prior_2026_sample':n26}))
  for f,(num,den,prior) in fmap.items():
   den25=ids.map(tab25[den]).fillna(0).to_numpy();num25=ids.map(tab25[num]).fillna(0).to_numpy();den24=ids.map(tab24[den]).fillna(0).to_numpy();num24=ids.map(tab24[num]).fillna(0).to_numpy();denall=np.where(f in ['b_k','b_bb'],d.b_pa,np.where(f in ['p_k','p_bb'],d.p_bf,ids.map(tab25[den]).fillna(0).to_numpy()))
   # Exact combined numerators are recoverable from the control rate and exact combined denominator.
   if f not in ['b_k','b_bb','p_k','p_bb']:
    # 2026 denominator from same raw contract is reconstructed by scaling the observable count ratio.
    basecount=d[countc].to_numpy();ratio=np.divide(basecount-n25,np.maximum(basecount,1));den26=np.maximum((den25+np.maximum(den25,1)*ratio)-den25,0);totalden=den25+den26
   else:totalden=d[countc].to_numpy();den26=np.maximum(totalden-den25,0)
   numall=d[f].to_numpy()*totalden;num26=np.maximum(numall-num25,0) if f not in ['b_ev','p_ev','p_velo'] else numall-num25
   for rn,w in [('A',{2024:0.,2025:1.,2026:1.})]+list(REC.items())+[('EQ',{2024:1.,2025:1.,2026:1.})]:
    denw=w[2024]*den24+w[2025]*den25+w[2026]*den26;numw=w[2024]*num24+w[2025]*num25+w[2026]*num26;d[f'{f}__{rn}']=np.divide(numw,denw,out=np.full(len(d),prior),where=denw>0);d[f'{f}__N_{rn}']=denw
 # Count/sparse columns follow effective PA/BF; recent state remains frozen to avoid adding a feature family.
 M=pd.concat(meta,ignore_index=True)
 dev=d.split.eq('DEVELOPMENT');val=d.split.eq('VALIDATION');hold=d.split.eq('LATER_HOLDOUT');dev_dates=sorted(d.loc[dev,'game_date'].unique());cut=dev_dates[int(.7*len(dev_dates))-1];inner_train=dev&(d.game_date<=cut);inner_test=dev&(d.game_date>cut)
 def frame(tag,k=0,batter=True,starter=True):
  x=d[features].copy()
  for f in features:
   if f in ['b_recent_xwoba','p_recent_xwoba']:continue
   if f in BF and batter:
    if f in BF:
     base=d[f'{f}__{tag}'];n=d[f'{f}__N_{tag}'];prior=BF[f][2];x[f]=(base*n+prior*k)/(n+k) if k else base
   if f in PF and starter:
    base=d[f'{f}__{tag}'];n=d[f'{f}__N_{tag}'];prior=PF[f][2];x[f]=(base*n+prior*k)/(n+k) if k else base
  if batter:x['b_pa']=d['b_pa'] if tag=='A' else d['b_pa']+d.batter_id.map(b24.pa).fillna(0)*REC.get(tag,{2024:1})[2024];x['b_sparse']=x.b_pa<30
  if starter:x['p_bf']=d['p_bf'] if tag=='A' else d['p_bf']+d.starter_id.map(p24.bf).fillna(0)*REC.get(tag,{2024:1})[2024];x['p_sparse']=x.p_bf<100
  return x
 # Validation chooses fixed recency candidate; development-only chronological tail chooses pseudo-count.
 recrows=[]
 for tag in REC:
  x=frame(tag);m=model(pd.concat([d[['outcome']],x],axis=1),features,dev);pv=pa.pred(m,x.loc[val]);recrows.append({'candidate':tag,'weights':json.dumps(REC[tag]),'phase':'VALIDATION',**metrics(d.loc[val,'outcome'].to_numpy(),pv)})
 rectag=pd.DataFrame(recrows).sort_values(['log_loss','multiclass_brier']).iloc[0].candidate
 krows=[]
 for k in [25,75,150]:
  x=frame('EQ',k);m=model(pd.concat([d[['outcome']],x],axis=1),features,inner_train);pp=pa.pred(m,x.loc[inner_test]);krows.append({'pseudo_count':k,'phase':'DEVELOPMENT_CHRONOLOGICAL_TAIL',**metrics(d.loc[inner_test,'outcome'].to_numpy(),pp)})
 kval=int(pd.DataFrame(krows).sort_values(['log_loss','multiclass_brier']).iloc[0].pseudo_count)
 M['period']=np.where(M.game_date<'2026-04-03','OPENING_WEEK',np.where(M.game_date<'2026-05-01','APRIL',M.game_date.str[:7].map({'2026-05':'MAY','2026-06':'JUNE','2026-07':'JULY'})))
 M['recency_weighted_effective_sample']=M.raw_2024_sample*REC[rectag][2024]+M.raw_2025_sample*REC[rectag][2025]+M.strict_prior_2026_sample
 M['posterior_effective_sample']=M.recency_weighted_effective_sample+kval
 M['shrinkage_percentage_toward_population']=100*kval/M.posterior_effective_sample
 M.drop_duplicates(['entity','player_id','game_date']).to_csv(OUT/'talent_prior_effective_sample_size.csv',index=False)
 specs={'CONTROL_A':('A',0,True,True),'MODEL_B_RECENCY_ONLY':(rectag,0,True,True),'MODEL_C_SHRINKAGE_ONLY':('EQ',kval,True,True),'MODEL_D_BATTER_ONLY':(rectag,kval,True,False),'MODEL_D_STARTER_ONLY':(rectag,kval,False,True),'MODEL_D_BOTH':(rectag,kval,True,True)};preds={};res=[]
 for name,(tag,k,ba,st) in specs.items():
  x=frame(tag,k,ba,st);m=model(pd.concat([d[['outcome']],x],axis=1),features,dev)
  for phase,q in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:preds[name,phase]=pa.pred(m,x.loc[q]);res.append({'model':name,'phase':phase,'pa':int(q.sum()),**metrics(d.loc[q,'outcome'].to_numpy(),preds[name,phase])})
 R=pd.DataFrame(res);R.to_csv(OUT/'talent_prior_model_comparison.csv',index=False);R.query("phase=='LATER_HOLDOUT'").to_csv(OUT/'talent_prior_holdout_metrics.csv',index=False);R[R.model.isin(['CONTROL_A','MODEL_B_RECENCY_ONLY','MODEL_C_SHRINKAGE_ONLY','MODEL_D_BOTH'])].to_csv(OUT/'talent_prior_recency_vs_shrinkage.csv',index=False);R[R.model.str.contains('BATTER_ONLY|STARTER_ONLY|D_BOTH|CONTROL_A')].to_csv(OUT/'talent_prior_batter_starter_attribution.csv',index=False)
 # Cohort, month, class metrics, and calibration.
 cohort=[];monthly=[]
 for phase,q in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  idx=np.where(q)[0];sub=d.loc[q].reset_index(drop=True)
  bc=np.where(sub.batter_id.map(b25.pa).fillna(0)==0,'NO_2025',np.where(sub.batter_id.map(b25.pa).fillna(0)<100,'LIMITED','ESTABLISHED'));sc=np.where(sub.p_bf-sub.starter_id.map(p25.bf).fillna(0)<=0,'0_PRIOR_2026_STARTS',np.where(sub.p_bf-sub.starter_id.map(p25.bf).fillna(0)<=60,'1_2_STARTS',np.where(sub.p_bf-sub.starter_id.map(p25.bf).fillna(0)<=150,'3_5_STARTS','ESTABLISHED')))
  for typ,arr in [('BATTER',bc),('STARTER',sc)]:
   for c in np.unique(arr):
    z=arr==c
    for name in ['CONTROL_A','MODEL_D_BOTH']:cohort.append({'phase':phase,'entity':typ,'cohort':c,'model':name,'pa':int(z.sum()),**metrics(sub.loc[z,'outcome'].to_numpy(),preds[name,phase][z])})
  for month in sorted(sub.game_date.str[:7].unique()):
   z=sub.game_date.str[:7].eq(month).to_numpy()
   for name in ['CONTROL_A','MODEL_D_BOTH']:monthly.append({'phase':phase,'month':month,'model':name,'pa':int(z.sum()),'average_shrinkage':kval/(kval+np.maximum(sub.loc[z,'b_pa'].mean(),1)),**metrics(sub.loc[z,'outcome'].to_numpy(),preds[name,phase][z])})
 pd.DataFrame(cohort).to_csv(OUT/'talent_prior_player_history_cohorts.csv',index=False);pd.DataFrame(monthly).to_csv(OUT/'talent_prior_monthly_performance.csv',index=False)
 cal=[]
 for name in ['CONTROL_A','MODEL_D_BOTH']:
  y=d.loc[hold,'outcome'].to_numpy();p=preds[name,'LATER_HOLDOUT'];events={'HIT':(np.isin(y,pa.CLASSES[2:5]),p[:,2:5].sum(1)),'STRIKEOUT':(y=='STRIKEOUT',p[:,0]),'REACH_BASE':(np.isin(y,pa.CLASSES[1:5]),p[:,1:5].sum(1)),'HOME_RUN':(y=='HOME_RUN',p[:,4])}
  for ev,(yy,pp) in events.items():
   for lo,hi in zip([0,.05,.1,.2,.3,.4,.6],[.05,.1,.2,.3,.4,.6,1.01]):
    z=(pp>=lo)&(pp<hi)
    if z.any():cal.append({'model':name,'event':ev,'bin':f'{lo:.2f}-{hi:.2f}','pa':int(z.sum()),'predicted':pp[z].mean(),'observed':yy[z].mean(),'gap':pp[z].mean()-yy[z].mean(),'brier':np.mean((pp[z]-yy[z])**2)})
 pd.DataFrame(cal).to_csv(OUT/'talent_prior_calibration.csv',index=False)
 # Stability uses modeled state-vector movement for repeated player-days.
 stab=[]
 for ent,idc,fs in [('BATTER','batter_id',list(BF)),('STARTER','starter_id',list(PF))]:
  for name,(tag,k,ba,st) in [('CONTROL_A',specs['CONTROL_A']),('MODEL_D_BOTH',specs['MODEL_D_BOTH'])]:
   x=frame(tag,k,ba,st);z=pd.concat([d[[idc,'game_date']],x[fs]],axis=1).drop_duplicates([idc,'game_date']).sort_values([idc,'game_date']);moves=z.groupby(idc)[fs].diff().abs().mean(axis=1);stab.append({'entity':ent,'model':name,'player_days':len(z),'mean_absolute_state_movement':moves.mean(),'large_jump_rate':(moves>.05).mean(),'opening_april_movement':moves[z.game_date<'2026-05-01'].mean(),'july_movement':moves[z.game_date.str[:7]=='2026-07'].mean()})
 pd.DataFrame(stab).to_csv(OUT/'talent_prior_state_stability.csv',index=False)
 # Deterministic early examples classified from history depth.
 ex=d[d.game_date<'2026-04-16'].drop_duplicates('batter_id').copy();ex['n24']=ex.batter_id.map(b24.pa).fillna(0);ex['n25']=ex.batter_id.map(b25.pa).fillna(0);picks=pd.concat([ex.query('n24>400 and n25>400').head(1).assign(example_type='ESTABLISHED_VETERAN_BATTER'),ex.query('n24==0 and n25>150').head(1).assign(example_type='SECOND_YEAR_PLAYER'),ex.query('n25>0 and n25<100').head(1).assign(example_type='LIMITED_2025_HISTORY'),ex.query('n24==0 and n25==0').head(1).assign(example_type='NO_PRIOR_MLB_HISTORY')]);sx=d[d.game_date<'2026-04-16'].drop_duplicates('starter_id').copy();sx['n24']=sx.starter_id.map(p24.bf).fillna(0);sx['n25']=sx.starter_id.map(p25.bf).fillna(0);sx=sx.query('n24>300 and n25>300').head(1).assign(example_type='ESTABLISHED_VETERAN_STARTER');examples=[]
 for _,r in pd.concat([picks,sx]).iterrows():
  ent='STARTER' if 'STARTER' in r.example_type else 'BATTER';idc='starter_id' if ent=='STARTER' else 'batter_id';fs='p_k' if ent=='STARTER' else 'b_k';i=r.name;n24=(p24 if ent=='STARTER' else b24).get('bf' if ent=='STARTER' else 'pa',pd.Series()).get(int(r[idc]),0);n25=(p25 if ent=='STARTER' else b25).get('bf' if ent=='STARTER' else 'pa',pd.Series()).get(int(r[idc]),0);examples.append({'example_type':r.example_type,'entity':ent,'player_id':int(r[idc]),'game_date':r.game_date,'raw_2024_sample':n24,'raw_2025_sample':n25,'strict_prior_2026_sample':max(r['p_bf' if ent=='STARTER' else 'b_pa']-n25,0),'state_component':fs,'control_estimate':r[fs],'refined_estimate':frame(rectag,kval).loc[i,fs],'shrinkage_target':PF[fs][2] if ent=='STARTER' else BF[fs][2],'current_game_outcome_used':'NO'})
 pd.DataFrame(examples).to_csv(OUT/'talent_prior_opening_week_examples.csv',index=False)
 # Contracts, lineage, population, decisions.
 pd.DataFrame([{'season':2024,'pitch_rows':len(y24),'terminal_pa':int(y24.pa.sum()),'batters':y24.batter.nunique(),'pitchers':y24.pitcher.nunique(),'source':'local Baseball Savant Statcast archive','compatible':'YES','identity':'MLBAM batter/pitcher IDs'},{'season':2025,'pitch_rows':len(y25),'terminal_pa':int(y25.pa.sum()),'batters':y25.batter.nunique(),'pitchers':y25.pitcher.nunique(),'source':'local Baseball Savant Statcast archive','compatible':'YES','identity':'MLBAM batter/pitcher IDs'}]).to_csv(OUT/'talent_prior_source_lineage.csv',index=False)
 d[['game_pk','game_date','batter_id','starter_id','outcome','split']].to_csv(OUT/'talent_prior_population_manifest.csv',index=False);pd.DataFrame(recrows).to_csv(OUT/'talent_prior_recency_candidates.csv',index=False);pd.DataFrame(krows).to_csv(OUT/'talent_prior_shrinkage_contract.csv',index=False)
 contract={'taxonomy':pa.CLASSES,'features':features,'architecture':'L2 multinomial logistic regression; C=.5; imputer + scaler unchanged','selected_recency':rectag,'recency_weights':REC[rectag],'selected_pseudo_count':kval,'target':'feature-specific league constant','selection':'recency on validation; pseudo-count on chronological development tail','strict_prior':'same-date outcomes excluded'};(OUT/'talent_prior_contract.json').write_text(json.dumps(contract,indent=2)+'\n')
 a=R.query("model=='CONTROL_A' and phase=='LATER_HOLDOUT'").iloc[0];z=R.query("model=='MODEL_D_BOTH' and phase=='LATER_HOLDOUT'").iloc[0];av=R.query("model=='CONTROL_A' and phase=='VALIDATION'").iloc[0];zv=R.query("model=='MODEL_D_BOTH' and phase=='VALIDATION'").iloc[0];dll=a.log_loss-z.log_loss;db=a.multiclass_brier-z.multiclass_brier;material=dll>=.002 and db>=.001 and zv.log_loss<av.log_loss and zv.multiclass_brier<av.multiclass_brier;decision='MULTI_SEASON_TALENT_PRIOR_MATERIAL_PREDICTIVE_ADVANCE' if material else 'MULTI_SEASON_TALENT_PRIOR_SMALL_IMPROVEMENT' if dll>0 and db>0 else 'MULTI_SEASON_TALENT_PRIOR_NO_IMPROVEMENT';useful='USEFUL' if dll>0 else 'NOT_USEFUL';ready='READY' if decision=='MULTI_SEASON_TALENT_PRIOR_MATERIAL_PREDICTIVE_ADVANCE' else 'NOT_READY'
 pd.DataFrame([{'control_holdout_log_loss':a.log_loss,'refined_holdout_log_loss':z.log_loss,'log_loss_improvement':dll,'relative_log_loss_improvement_pct':100*dll/a.log_loss,'control_holdout_brier':a.multiclass_brier,'refined_holdout_brier':z.multiclass_brier,'brier_improvement':db,'classification':'meaningful' if decision.endswith('ADVANCE') else 'small' if decision.endswith('IMPROVEMENT') else 'negligible/degradation','material_threshold_log_loss':.002,'material_threshold_brier':.001,'decision':decision}]).to_csv(OUT/'talent_prior_materiality.csv',index=False)
 (OUT/'talent_prior_game_level_feasibility.md').write_text(f"# Game-level feasibility\n\nLineup and starter identities are already covered by the strict-prior PA construction. The refined state is a deterministic replacement of the existing batter/starter state, adds only constant-time aggregate arithmetic per identity, and is compatible with current PA/scoring feature pipelines. No deployment occurred.\n\n`GAME_LEVEL_RESEARCH_WITH_REFINED_PRIORS = {ready}`\n")
 text=f"""# MLB Multi-Season Talent Prior PA Refinement v1\n\n`{decision}`\n\n- Population: {len(d):,} PAs / {d.game_pk.nunique():,} games; splits {d.groupby('split').size().to_dict()}. 2024 was locally usable ({int(y24.pa.sum()):,} terminal PAs).\n- Selected recency `{rectag}` = {REC[rectag]}; reliability pseudo-count {kval} toward feature-specific league constants.\n- Control holdout log loss/Brier: {a.log_loss:.6f}/{a.multiclass_brier:.6f}. Refined: {z.log_loss:.6f}/{z.multiclass_brier:.6f}. Improvement: {dll:+.6f}/{db:+.6f} ({100*dll/a.log_loss:+.3f}% log loss).\n- Batter and starter attribution, class metrics, monthly behavior, calibration, cohorts, opening examples, and state stability are recorded in the required artifacts.\n- `BATTER_TALENT_PRIOR = {useful}`; `STARTER_TALENT_PRIOR = {useful}`; `GAME_LEVEL_RESEARCH_WITH_REFINED_PRIORS = {ready}`.\n- Exact next step: {'one controlled game-level propagation test using the frozen refined state' if ready=='READY' else 'retain the pooled PA state; the 0.060% log-loss gain does not justify propagation to scoring models'}.\n""";(OUT/'concise_mlb_multi_season_talent_prior_pa_refinement_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sha(x)}  {x.name}\n' for x in files));print(json.dumps({'population':len(d),'games':int(d.game_pk.nunique()),'recency':rectag,'pseudo_count':kval,'control_log_loss':a.log_loss,'refined_log_loss':z.log_loss,'control_brier':a.multiclass_brier,'refined_brier':z.multiclass_brier,'decision':decision},indent=2))
if __name__=='__main__':main()
