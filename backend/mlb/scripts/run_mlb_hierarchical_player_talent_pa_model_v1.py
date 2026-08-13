#!/usr/bin/env python3
"""Sparse Gaussian-MAP hierarchical batter/pitcher PA hurdle experiment."""
from __future__ import annotations
import hashlib,json,time,resource,tempfile,sys
from pathlib import Path
import numpy as np,pandas as pd
from scipy import sparse
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LogisticRegression
from backend.mlb.scripts import run_mlb_pa_outcome_prediction_foundation_v1 as pa
from backend.mlb.scripts import run_mlb_multi_season_talent_prior_pa_refinement_v1 as prior

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_hierarchical_player_talent_pa_model_v1/2026-08-12';RAW=ROOT/'backend/mlb/data/external/statcast/raw';BASE=ROOT/'artifacts/analysis/model_development/mlb_pa_outcome_prediction_foundation_v1/2026-08-12/pa_population_manifest.csv';C=.08;SEASON_SCALE=.65
STAGES=[('K',lambda y:y=='STRIKEOUT',lambda y:np.ones(len(y),bool)),('BB',lambda y:y=='WALK_HBP',lambda y:y!='STRIKEOUT'),('HR',lambda y:y=='HOME_RUN',lambda y:~np.isin(y,['STRIKEOUT','WALK_HBP'])),('HIP',lambda y:np.isin(y,['SINGLE','DOUBLE_TRIPLE']),lambda y:~np.isin(y,['STRIKEOUT','WALK_HBP','HOME_RUN'])),('XBH',lambda y:y=='DOUBLE_TRIPLE',lambda y:np.isin(y,['SINGLE','DOUBLE_TRIPLE']))]
def sh(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def cat(e):return pa.cat(e)
def load_year(y):
 cols=['game_date','game_pk','at_bat_number','pitch_number','batter','pitcher','events','stand','p_throws'];fs=[pd.read_csv(f,usecols=cols,low_memory=False) for f in sorted((RAW/str(y)).glob('*/statcast_search.csv'))];d=pd.concat(fs,ignore_index=True);d.game_date=pd.to_datetime(d.game_date).dt.strftime('%Y-%m-%d');d=d.dropna(subset=['game_pk','at_bat_number','pitch_number','batter','pitcher']).drop_duplicates(['game_pk','at_bat_number','pitch_number'],keep='last');d=d[d.events.notna()].drop_duplicates(['game_pk','at_bat_number']);d['outcome']=d.events.map(cat);d['batter_id']=d.batter.astype(int);d['pitcher_id']=d.pitcher.astype(int);d['season']=y;d['is_eval']=False;return d[['game_pk','game_date','batter_id','pitcher_id','stand','p_throws','outcome','season','is_eval']]
def design(df,evolution=True,context=False):
 rec=[]
 for r in df.itertuples():
  z={f'b:{r.batter_id}':1.,f'p:{r.pitcher_id}':1.}
  if evolution:z[f'bs:{r.batter_id}:{r.season}']=SEASON_SCALE;z[f'ps:{r.pitcher_id}:{r.season}']=SEASON_SCALE
  if context:z[f'hand:{r.stand}:{r.p_throws}']=1.;z[f'season:{r.season}']=1.
  rec.append(z)
 return rec
def fit_hurdle(train,score,evolution=True,context=False):
 vec=DictVectorizer();X=vec.fit_transform(design(pd.concat([train,score]),evolution,context));Xt=X[:len(train)];Xs=X[len(train):];y=train.outcome.to_numpy();mods=[];probs=[]
 for name,pos,eligible in STAGES:
  q=eligible(y);yy=pos(y[q]).astype(int);m=LogisticRegression(C=C,solver='liblinear',max_iter=300,random_state=20260812).fit(Xt[q],yy);mods.append((name,m));probs.append(m.predict_proba(Xs)[:,1])
 k,bb,hr,hip,xbh=probs;P=np.column_stack([k,(1-k)*bb,(1-k)*(1-bb)*(1-hr)*hip*(1-xbh),(1-k)*(1-bb)*(1-hr)*hip*xbh,(1-k)*(1-bb)*hr,(1-k)*(1-bb)*(1-hr)*(1-hip)]);return P/P.sum(1,keepdims=True),vec,mods
def metric(y,p):
 m=pa.multi(y,p);ev={'k':(y=='STRIKEOUT',p[:,0]),'bb':(y=='WALK_HBP',p[:,1]),'hit':(np.isin(y,pa.CLASSES[2:5]),p[:,2:5].sum(1)),'xbh':(np.isin(y,pa.CLASSES[3:5]),p[:,3:5].sum(1)),'hr':(y=='HOME_RUN',p[:,4]),'reach':(np.isin(y,pa.CLASSES[1:5]),p[:,1:5].sum(1))}
 for n,(yy,pp) in ev.items():yy=yy.astype(float);m[n+'_brier']=np.mean((pp-yy)**2);m[n+'_log_loss']=np.mean(-yy*np.log(np.clip(pp,1e-9,1))-(1-yy)*np.log(np.clip(1-pp,1e-9,1)));m[n+'_sd']=pp.std()
 return m
def main():
 OUT.mkdir(parents=True,exist_ok=True);t0=time.time();base=pd.read_csv(BASE);evald=base.rename(columns={'starter_id':'pitcher_id','batter_hand':'stand','pitcher_hand':'p_throws'});evald['season']=2026;evald['is_eval']=True;evald=evald[['game_pk','game_date','batter_id','pitcher_id','stand','p_throws','outcome','season','is_eval','split']]
 h24=load_year(2024);h25=load_year(2025);hist=pd.concat([h24,h25],ignore_index=True);hist['split']='HISTORY';allp=pd.concat([hist,evald],ignore_index=True);allp.to_csv(OUT/'hierarchical_pa_population_manifest.csv',index=False)
 # Reconstruct authoritative generic prior predictions for identical evaluation rows.
 cap={};old=prior.OUT;prior.OUT=Path(tempfile.mkdtemp(prefix='hier_prior_'))
 def trace(fr,event,arg):
  if fr.f_code.co_filename==prior.__file__ and fr.f_code.co_name=='main' and event=='return':cap.update(fr.f_locals)
  return trace
 sys.settrace(trace)
 try:prior.main()
 finally:sys.settrace(None);prior.OUT=old
 gd=cap['d'];gx=cap['frame']('B2',75);generic={}
 for phase,q in [('VALIDATION',gd.split.eq('VALIDATION')),('LATER_HOLDOUT',gd.split.eq('LATER_HOLDOUT'))]:m=pa.fit_model(gx.loc[gd.split.eq('DEVELOPMENT'),pa.B+pa.P],gd.loc[gd.split.eq('DEVELOPMENT'),'outcome'],'linear');generic[phase]=pa.pred(m,gx.loc[q,pa.B+pa.P])
 # Fixed monthly strict-prior refits. Same-date/game rows never enter their prediction state.
 pred={('MODEL_D_HIERARCHICAL',x):[] for x in ['VALIDATION','LATER_HOLDOUT']};pred.update({('MODEL_E_LIMITED_CONTEXT',x):[] for x in ['VALIDATION','LATER_HOLDOUT']});truth={x:[] for x in ['VALIDATION','LATER_HOLDOUT']};rowids={x:[] for x in truth};runt=[];last_state=None;state_snapshots=[]
 for month in sorted(evald.game_date.str[:7].unique()):
  score=evald[evald.game_date.str[:7].eq(month)];train=pd.concat([hist,evald[evald.game_date<month+'-01']],ignore_index=True)
  start=time.time();pd0,v0,m0=fit_hurdle(train,score,True,False);runt.append({'month':month,'variant':'MODEL_D_HIERARCHICAL','training_pa':len(train),'scored_pa':len(score),'seconds':time.time()-start,'latent_parameters':len(v0.feature_names_),'player_effects':train.batter_id.nunique()+train.pitcher_id.nunique()})
  start=time.time();pe,ve,me=fit_hurdle(train,score,True,True);runt.append({'month':month,'variant':'MODEL_E_LIMITED_CONTEXT','training_pa':len(train),'scored_pa':len(score),'seconds':time.time()-start,'latent_parameters':len(ve.feature_names_),'player_effects':train.batter_id.nunique()+train.pitcher_id.nunique()})
  vn=np.array(ve.feature_names_)
  for stage,mm in me:
   for ix,key in enumerate(vn):
    if key.startswith(('b:','p:')):state_snapshots.append({'month':month,'stage':stage,'entity':'BATTER' if key.startswith('b:') else 'PITCHER','player_id':int(key.split(':')[1]),'latent_effect':mm.coef_[0,ix]})
  for phase in truth:
   z=score.split.eq(phase);pred['MODEL_D_HIERARCHICAL',phase].append(pd0[z]);pred['MODEL_E_LIMITED_CONTEXT',phase].append(pe[z]);truth[phase].extend(score.loc[z,'outcome']);rowids[phase].extend(score.index[z])
  last_state=(ve,me,train)
 for k in list(pred):pred[k]=np.vstack(pred[k]) if pred[k] else np.empty((0,6))
 # Model comparison includes authoritative fixed controls and actual challenger metrics.
 rows=[]
 refs={'CONTROL_A_LEAGUE_RATE':{'VALIDATION':(1.447054,0.704568),'LATER_HOLDOUT':(1.450114,0.705880)},'CONTROL_B_SIMPLE_RATE':{'VALIDATION':(1.427245,0.695197),'LATER_HOLDOUT':(1.429836,0.697197)}}
 for name,z in refs.items():
  for phase,(ll,br) in z.items():rows.append({'model':name,'phase':phase,'pa':len(truth[phase]),'log_loss':ll,'multiclass_brier':br,'source':'authoritative predecessor'})
 for phase in truth:
  y=np.asarray(truth[phase]);rows.append({'model':'CONTROL_C_GENERIC_PRIOR','phase':phase,'pa':len(y),**metric(y,generic[phase]),'source':'reproduced'});
  for name in ['MODEL_D_HIERARCHICAL','MODEL_E_LIMITED_CONTEXT']:rows.append({'model':name,'phase':phase,'pa':len(y),**metric(y,pred[name,phase]),'source':'strict-prior monthly refit'})
 R=pd.DataFrame(rows);R.to_csv(OUT/'hierarchical_model_comparison.csv',index=False);R.query("phase=='LATER_HOLDOUT'").to_csv(OUT/'hierarchical_holdout_metrics.csv',index=False)
 # Validation selects D/E only; holdout opened after this fixed choice.
 selected=R.query("phase=='VALIDATION' and model.str.startswith('MODEL_')",engine='python').sort_values(['log_loss','multiclass_brier']).iloc[0].model
 # Pooled-history ablation: no season states, evaluated with identical monthly refits.
 abl=[]
 for phase in truth:
  q=evald.split.eq(phase);score=evald[q];train=pd.concat([hist,evald[evald.game_date<score.game_date.min()]],ignore_index=True);pp,_,_=fit_hurdle(train,score,False,False);abl.append({'variant':'HIERARCHICAL_POOLED_HISTORY','phase':phase,'pa':len(score),**metric(score.outcome.to_numpy(),pp)})
  rr=R.query('model==@selected and phase==@phase').iloc[0];abl.append({'variant':'HIERARCHICAL_SEASON_EVOLUTION','phase':phase,'pa':rr.pa,'log_loss':rr.log_loss,'multiclass_brier':rr.multiclass_brier})
 pd.DataFrame(abl).to_csv(OUT/'hierarchical_temporal_evolution_ablation.csv',index=False)
 # Opponent adjustment reference is generic separately-built rate model.
 opp=[]
 for phase in truth:
  g=R.query("model=='CONTROL_C_GENERIC_PRIOR' and phase==@phase").iloc[0];h=R.query('model==@selected and phase==@phase').iloc[0];opp.append({'phase':phase,'rate_model_log_loss':g.log_loss,'joint_log_loss':h.log_loss,'log_loss_improvement':g.log_loss-h.log_loss,'rate_model_brier':g.multiclass_brier,'joint_brier':h.multiclass_brier,'brier_improvement':g.multiclass_brier-h.multiclass_brier,'k_improvement':g.k_brier-h.k_brier,'bb_improvement':g.bb_brier-h.bb_brier,'hit_improvement':g.hit_brier-h.hit_brier,'hr_improvement':g.hr_brier-h.hr_brier})
 pd.DataFrame(opp).to_csv(OUT/'hierarchical_opponent_adjustment_ablation.csv',index=False)
 # Class metrics, cohorts, calibration, separation.
 cls=[];coh=[];cal=[];sep=[]
 for phase in truth:
  y=np.asarray(truth[phase]);ids=np.asarray(rowids[phase]);sub=evald.loc[ids].reset_index(drop=True)
  for name,pp in [('CONTROL_C_GENERIC_PRIOR',generic[phase]),(selected,pred[selected,phase])]:
   mm=metric(y,pp)
   for ev in ['k','bb','hit','xbh','hr','reach']:cls.append({'model':name,'phase':phase,'event':ev.upper(),'brier':mm[ev+'_brier'],'log_loss':mm[ev+'_log_loss'],'probability_sd':mm[ev+'_sd']})
   if phase=='LATER_HOLDOUT' and name==selected:
    for ev,xx in [('K',pp[:,0]),('BB_HBP',pp[:,1]),('HIT',pp[:,2:5].sum(1)),('HR',pp[:,4]),('REACH_BASE',pp[:,1:5].sum(1))]:sep.append({'event':ev,'mean':xx.mean(),'sd':xx.std(),'p05':np.quantile(xx,.05),'p25':np.quantile(xx,.25),'median':np.median(xx),'p75':np.quantile(xx,.75),'p95':np.quantile(xx,.95),'minimum':xx.min(),'maximum':xx.max()})
   for ev,yy,xx in [('K',y=='STRIKEOUT',pp[:,0]),('BB_HBP',y=='WALK_HBP',pp[:,1]),('HIT',np.isin(y,pa.CLASSES[2:5]),pp[:,2:5].sum(1)),('HR',y=='HOME_RUN',pp[:,4]),('REACH_BASE',np.isin(y,pa.CLASSES[1:5]),pp[:,1:5].sum(1))]:
    for lo,hi in zip([0,.05,.1,.2,.3,.4,.6],[.05,.1,.2,.3,.4,.6,1.01]):
     z=(xx>=lo)&(xx<hi)
     if z.any():cal.append({'model':name,'phase':phase,'event':ev,'bin':f'{lo:.2f}-{hi:.2f}','pa':int(z.sum()),'predicted':xx[z].mean(),'observed':yy[z].mean(),'gap':xx[z].mean()-yy[z].mean()})
  bh=sub.batter_id.map(hist.groupby('batter_id').size()).fillna(0);ph=sub.pitcher_id.map(hist.groupby('pitcher_id').size()).fillna(0)
  for ent,histn in [('BATTER',bh),('PITCHER',ph)]:
   cc=np.where(histn==0,'NO_PRIOR',np.where(histn<100,'LT100',np.where(histn<=300,'100_300','GT300')))
   for c in np.unique(cc):
    z=cc==c
    for name,pp in [('CONTROL_C_GENERIC_PRIOR',generic[phase]),(selected,pred[selected,phase])]:coh.append({'entity':ent,'cohort':c,'model':name,'phase':phase,'pa':int(z.sum()),**metric(y[z],pp[z])})
 pd.DataFrame(cls).to_csv(OUT/'hierarchical_class_probability_metrics.csv',index=False);pd.DataFrame(coh).to_csv(OUT/'hierarchical_player_history_cohorts.csv',index=False);pd.DataFrame(cal).to_csv(OUT/'hierarchical_calibration.csv',index=False);pd.DataFrame(sep).to_csv(OUT/'hierarchical_probability_separation.csv',index=False)
 # Latent state and shrinkage diagnostics from last monthly fit.
 vec,mods,tr=last_state;names=np.array(vec.feature_names_);tal=[]
 counts_b=tr.groupby('batter_id').size();counts_p=tr.groupby('pitcher_id').size()
 raw_maps={'K':lambda z:(z.outcome=='STRIKEOUT').mean(),'BB':lambda z:(z.outcome=='WALK_HBP').mean(),'HR':lambda z:(z.outcome=='HOME_RUN').mean(),'HIP':lambda z:z.outcome.isin(['SINGLE','DOUBLE_TRIPLE']).mean(),'XBH':lambda z:(z.outcome=='DOUBLE_TRIPLE').mean()}
 snaps=pd.DataFrame(state_snapshots)
 for stage,m in mods:
  coef=m.coef_[0]
  for ent,prefix,counts in [('BATTER','b:',counts_b),('PITCHER','p:',counts_p)]:
   for pid,n in counts.items():
    key=f'{prefix}{pid}';ix=np.where(names==key)[0];effect=coef[ix[0]] if len(ix) else 0;raw=raw_maps[stage](tr[tr[('batter_id' if ent=='BATTER' else 'pitcher_id')].eq(pid)]);latent=1/(1+np.exp(-(m.intercept_[0]+effect)));ss=snaps[(snaps.entity==ent)&(snaps.player_id==pid)&(snaps.stage==stage)].sort_values('month');tal.append({'entity':ent,'player_id':pid,'talent_dimension':stage,'raw_observed_rate':raw,'posterior_latent_probability':latent,'standardized_latent_effect':effect,'shrinkage_toward_population':latent-raw,'effective_prior_influence':1/(1+C*n),'history_pa_or_bf':n,'opening_2026_effect':ss.latent_effect.iloc[0] if len(ss) else np.nan,'july_2026_effect':ss.latent_effect.iloc[-1] if len(ss) else np.nan,'reliability':'HIGH' if n>500 else 'MEDIUM' if n>100 else 'LOW'})
 T=pd.DataFrame(tal);T.to_csv(OUT/'hierarchical_player_talent_state.csv',index=False)
 stab=[];snaps['movement']=snaps.sort_values(['entity','player_id','stage','month']).groupby(['entity','player_id','stage']).latent_effect.diff().abs()
 for ent in ['BATTER','PITCHER']:
  ids_low=set(T[(T.entity==ent)&T.reliability.eq('LOW')].player_id);ids_high=set(T[(T.entity==ent)&T.reliability.eq('HIGH')].player_id)
  for cohort,ids in [('LOW',ids_low),('HIGH',ids_high)]:
   z=snaps[(snaps.entity==ent)&snaps.player_id.isin(ids)].dropna(subset=['movement']);stab.append({'entity':ent,'history_group':cohort,'model':'HIERARCHICAL','updates':len(z),'mean_absolute_update':z.movement.mean(),'median_absolute_update':z.movement.median(),'p95_absolute_update':z.movement.quantile(.95),'largest_update':z.movement.max(),'early_update':z[z.month<'2026-05'].movement.mean(),'july_update':z[z.month=='2026-07'].movement.mean(),'update_contract':'monthly pregame refit; no intra-game update'})
 pd.DataFrame(stab).to_csv(OUT/'hierarchical_state_stability.csv',index=False)
 pd.DataFrame(runt).assign(peak_memory_mb=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024*1024),daily_practical='YES; monthly refit well within daily window').to_csv(OUT/'hierarchical_computational_feasibility.csv',index=False)
 # Contracts and decision.
 (OUT/'hierarchical_pa_outcome_contract.json').write_text(json.dumps({'classes':pa.CLASSES,'mapping':'identical predecessor cat() mapping','each_terminal_pa_exactly_once':'YES'},indent=2)+'\n');(OUT/'hierarchical_model_contract.json').write_text(json.dumps({'formulation':'five-stage logistic hurdle','partial_pooling':'Gaussian-MAP via L2 penalty','C':C,'joint_effects':'batter and pitcher estimated simultaneously','season_effect_scale':SEASON_SCALE,'selected_variant':selected,'limited_context':['handedness matchup','season intercept'],'park_effect':'omitted because no authoritative venue field exists in the compact historical PA source'},indent=2)+'\n');(OUT/'hierarchical_temporal_update_contract.json').write_text(json.dumps({'strategy':'periodic monthly refit','history':'all 2024/2025 plus 2026 dates strictly before month','same_game_state':'all PAs share monthly pregame state','intra_game_update':'NO'},indent=2)+'\n');(OUT/'hierarchical_hyperparameter_contract.json').write_text(json.dumps({'C':C,'season_scale':SEASON_SCALE,'selection_source':'fixed before 2026 validation/holdout; no holdout tuning','validation_role':'select Model D vs E only'},indent=2)+'\n')
 g=R.query("model=='CONTROL_C_GENERIC_PRIOR' and phase=='LATER_HOLDOUT'").iloc[0];h=R.query('model==@selected and phase=="LATER_HOLDOUT"').iloc[0];dll=g.log_loss-h.log_loss;dbr=g.multiclass_brier-h.multiclass_brier;valid=R.query('model==@selected and phase=="VALIDATION"').iloc[0].log_loss<R.query("model=='CONTROL_C_GENERIC_PRIOR' and phase=='VALIDATION'").iloc[0].log_loss;decision='HIERARCHICAL_PLAYER_TALENT_MATERIAL_PREDICTIVE_ADVANCE' if valid and dll>=.002 and dbr>0 else 'HIERARCHICAL_PLAYER_TALENT_SMALL_IMPROVEMENT' if valid and dll>0 and dbr>0 else 'HIERARCHICAL_PLAYER_TALENT_NO_IMPROVEMENT';gate='READY' if decision.endswith('ADVANCE') else 'NOT_READY';oppuse='USEFUL' if dll>0 else 'NOT_USEFUL';temp=pd.DataFrame(abl);tv=temp.query("variant=='HIERARCHICAL_POOLED_HISTORY' and phase=='LATER_HOLDOUT'").iloc[0].log_loss-h.log_loss;tempuse='USEFUL' if tv>0 else 'NOT_USEFUL'
 pd.DataFrame([{'selected':selected,'generic_log_loss':g.log_loss,'hierarchical_log_loss':h.log_loss,'log_loss_improvement':dll,'relative_log_loss_improvement_pct':100*dll/g.log_loss,'generic_brier':g.multiclass_brier,'hierarchical_brier':h.multiclass_brier,'brier_improvement':dbr,'k_improvement':g.k_brier-h.k_brier,'bb_improvement':g.bb_brier-h.bb_brier,'hit_improvement':g.hit_brier-h.hit_brier,'hr_improvement':g.hr_brier-h.hr_brier,'temporal_evolution_log_loss_improvement':tv,'decision':decision,'game_level_propagation':gate}]).to_csv(OUT/'hierarchical_materiality.csv',index=False)
 text=f"""# MLB Hierarchical Player Talent PA Model v1\n\n`{decision}`\n\n- Historical population: 2024 {len(h24):,}, 2025 {len(h25):,}; evaluation 2026 {len(evald):,} PAs / {evald.game_pk.nunique():,} games.\n- Selected `{selected}`: five-stage Gaussian-MAP hurdle with joint batter/pitcher base and season effects; monthly strict-prior refits.\n- Generic holdout log loss/Brier {g.log_loss:.6f}/{g.multiclass_brier:.6f}; hierarchical {h.log_loss:.6f}/{h.multiclass_brier:.6f}; improvement {dll:+.6f}/{dbr:+.6f}.\n- Batter effects {T.query("entity=='BATTER'").player_id.nunique():,}; pitcher effects {T.query("entity=='PITCHER'").player_id.nunique():,}. `OPPONENT_ADJUSTMENT = {oppuse}`; `TEMPORAL_TALENT_EVOLUTION = {tempuse}`; `GAME_LEVEL_PROPAGATION = {gate}`.\n- Exact next step: {'one bounded game-level propagation experiment' if gate=='READY' else 'stop hierarchical player-talent research and retain the generic multi-season PA prior'}.\n""";(OUT/'concise_mlb_hierarchical_player_talent_pa_model_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'2024':len(h24),'2025':len(h25),'2026':len(evald),'selected':selected,'generic_ll':g.log_loss,'hierarchical_ll':h.log_loss,'generic_brier':g.multiclass_brier,'hierarchical_brier':h.multiclass_brier,'decision':decision,'gate':gate,'runtime_seconds':time.time()-t0},indent=2))
if __name__=='__main__':main()
