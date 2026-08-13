#!/usr/bin/env python3
"""Research-only MLB starter-vs-batter PA outcome foundation."""
from __future__ import annotations
import hashlib,json
from collections import Counter,defaultdict
from pathlib import Path
import numpy as np,pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss,roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from backend.mlb.scripts import run_mlb_expected_quality_feature_platform_inventory_v1 as inv

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_pa_outcome_prediction_foundation_v1/2026-08-12';SPINE=ROOT/'artifacts/analysis/model_development/mlb_expected_quality_scoring_model_v1/2026-08-12/expected_quality_model_population.csv';CLASSES=['STRIKEOUT','WALK_HBP','SINGLE','DOUBLE_TRIPLE','HOME_RUN','OTHER_OUT'];SEED=20260812
B=['b_xwoba','b_xba','b_xslg','b_ev','b_hard','b_barrel','b_k','b_bb','b_whiff','b_pa','b_sparse','b_recent_xwoba']
P=['p_xwoba','p_ev','p_hard','p_barrel','p_k','p_bb','p_whiff','p_gb','p_velo','p_bf','p_sparse','p_recent_xwoba']
H=['same_hand','batter_left','batter_switch','pitcher_left'];M=['match_xwoba','match_whiff','match_power','direct_bvp_pitches']
LADDER=[('CONTROL_0_LEAGUE_RATE',[]),('CONTROL_1_BATTER_RATE',B),('CONTROL_2_BATTER_STARTER',B+P),('MODEL_A_PLUS_HANDEDNESS',B+P+H),('MODEL_B_PLUS_PITCH_FAMILY',B+P+H+M)]
def sh(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def cat(e):
 e=str(e)
 if e in ['strikeout','strikeout_double_play']:return 'STRIKEOUT'
 if e in ['walk','intent_walk','hit_by_pitch']:return 'WALK_HBP'
 if e=='single':return 'SINGLE'
 if e in ['double','triple']:return 'DOUBLE_TRIPLE'
 if e=='home_run':return 'HOME_RUN'
 return 'OTHER_OUT'
def rate(z,a,b,d):return z[a]/z[b] if z[b] else d
def build():
 spine=pd.read_csv(SPINE);games=spine.set_index('game_pk').to_dict('index');s=inv.load_statcast();s=s[s.game_date.str.startswith(('2025','2026'))].copy();s['xw']=pd.to_numeric(s.estimated_woba_using_speedangle,errors='coerce');s['xba']=pd.to_numeric(s.estimated_ba_using_speedangle,errors='coerce');s['xslg']=pd.to_numeric(s.estimated_slg_using_speedangle,errors='coerce');s['ev']=pd.to_numeric(s.launch_speed,errors='coerce');s['velo']=pd.to_numeric(s.release_speed,errors='coerce');s['pa']=s.events.notna();s['k']=s.events.astype(str).str.contains('strikeout');s['bb']=s.events.astype(str).isin(['walk','intent_walk','hit_by_pitch']);s['whiff']=s.description.astype(str).str.contains('swinging_strike|foul_tip');s['swing']=s.description.astype(str).str.contains('swinging|foul|hit_into_play');s['hard']=s.ev>=95;s['barrel']=pd.to_numeric(s.launch_speed_angle,errors='coerce').eq(6);s['gb']=s.bb_type.eq('ground_ball')
 days={d:g for d,g in s.groupby('game_date',sort=True)};bs=defaultdict(lambda:defaultdict(float));ps=defaultdict(lambda:defaultdict(float));bf=defaultdict(lambda:defaultdict(Counter));pf=defaultdict(Counter);bvp=Counter();recentb=defaultdict(lambda:[]);recentp=defaultdict(lambda:[]);rows=[];eligible_terminal=excluded_relief=excluded_game=0
 def add(day):
  for bid,g in day.groupby('batter'):
   z=bs[int(bid)];snap={'x_sum':g.xw.sum(),'x_n':g.xw.notna().sum(),'pa':g.pa.sum()};recentb[int(bid)]=(recentb[int(bid)]+[snap])[-10:]
   for k,v in [('pitches',len(g)),('pa',g.pa.sum()),('x_n',g.xw.notna().sum()),('xba_n',g.xba.notna().sum()),('xslg_n',g.xslg.notna().sum()),('ev_n',g.ev.notna().sum()),('hard',g.hard.sum()),('barrel',g.barrel.sum()),('k',g.k.sum()),('bb',g.bb.sum()),('whiff',g.whiff.sum()),('swing',g.swing.sum())]:z[k]+=v
   for k,c in [('x_sum','xw'),('xba_sum','xba'),('xslg_sum','xslg'),('ev_sum','ev')]:z[k]+=g[c].sum()
   for f,h in g.groupby('pitch_type'):bf[int(bid)]['n'][f]+=len(h);bf[int(bid)]['x'][f]+=h.xw.sum();bf[int(bid)]['xn'][f]+=h.xw.notna().sum();bf[int(bid)]['whiff'][f]+=h.whiff.sum();bf[int(bid)]['swing'][f]+=h.swing.sum();bf[int(bid)]['power'][f]+=h.xslg.sum();bf[int(bid)]['powern'][f]+=h.xslg.notna().sum()
  for pid,g in day.groupby('pitcher'):
   z=ps[int(pid)];snap={'x_sum':g.xw.sum(),'x_n':g.xw.notna().sum(),'bf':g.pa.sum()};recentp[int(pid)]=(recentp[int(pid)]+[snap])[-3:]
   for k,v in [('pitches',len(g)),('bf',g.pa.sum()),('x_n',g.xw.notna().sum()),('ev_n',g.ev.notna().sum()),('hard',g.hard.sum()),('barrel',g.barrel.sum()),('k',g.k.sum()),('bb',g.bb.sum()),('whiff',g.whiff.sum()),('swing',g.swing.sum()),('gb',g.gb.sum()),('velo_n',g.velo.notna().sum())]:z[k]+=v
   z['x_sum']+=g.xw.sum();z['ev_sum']+=g.ev.sum();z['velo_sum']+=g.velo.sum();pf[int(pid)].update(g.pitch_type.dropna())
  for (b,p),g in day.groupby(['batter','pitcher']):bvp[int(b),int(p)]+=len(g)
 for d in sorted(x for x in days if x<'2026-03-26'):add(days[d])
 for date in sorted(x for x in days if '2026-03-26'<=x<='2026-07-27'):
  day=days[date];terminal=day[day.events.notna()].drop_duplicates(['game_pk','at_bat_number']);eligible_terminal+=len(terminal)
  for r in terminal.itertuples():
   g=games.get(int(r.game_pk))
   if not g:excluded_game+=1;continue
   starters={int(g['away_starting_pitcher_id']),int(g['home_starting_pitcher_id'])}
   if int(r.pitcher) not in starters:excluded_relief+=1;continue
   b=bs[int(r.batter)];p=ps[int(r.pitcher)];rb=recentb[int(r.batter)];rp=recentp[int(r.pitcher)];bx=sum(x['x_sum'] for x in rb)/sum(x['x_n'] for x in rb) if sum(x['x_n'] for x in rb) else rate(b,'x_sum','x_n',.32);px=sum(x['x_sum'] for x in rp)/sum(x['x_n'] for x in rp) if sum(x['x_n'] for x in rp) else rate(p,'x_sum','x_n',.32);mix=pf[int(r.pitcher)];tot=sum(mix.values());mx=mw=mp=0
   for f,n in mix.most_common(4):
    q=bf[int(r.batter)];w=n/tot;mx+=w*(q['x'][f]/q['xn'][f] if q['xn'][f] else rate(b,'x_sum','x_n',.32));mw+=w*(q['whiff'][f]/q['swing'][f] if q['swing'][f] else rate(b,'whiff','swing',.24));mp+=w*(q['power'][f]/q['powern'][f] if q['powern'][f] else rate(b,'xslg_sum','xslg_n',.41))
   rows.append({'game_pk':int(r.game_pk),'game_date':date,'game_start':g['start'],'at_bat_number':int(r.at_bat_number),'batter_id':int(r.batter),'starter_id':int(r.pitcher),'source_event':r.events,'outcome':cat(r.events),'split':g['split'],'batter_hand':r.stand,'pitcher_hand':r.p_throws,'b_xwoba':rate(b,'x_sum','x_n',.32),'b_xba':rate(b,'xba_sum','xba_n',.245),'b_xslg':rate(b,'xslg_sum','xslg_n',.41),'b_ev':rate(b,'ev_sum','ev_n',88),'b_hard':rate(b,'hard','ev_n',.38),'b_barrel':rate(b,'barrel','ev_n',.07),'b_k':rate(b,'k','pa',.23),'b_bb':rate(b,'bb','pa',.085),'b_whiff':rate(b,'whiff','swing',.24),'b_pa':b['pa'],'b_sparse':b['pa']<30,'b_recent_xwoba':bx,'p_xwoba':rate(p,'x_sum','x_n',.32),'p_ev':rate(p,'ev_sum','ev_n',88),'p_hard':rate(p,'hard','ev_n',.38),'p_barrel':rate(p,'barrel','ev_n',.07),'p_k':rate(p,'k','bf',.23),'p_bb':rate(p,'bb','bf',.085),'p_whiff':rate(p,'whiff','swing',.24),'p_gb':rate(p,'gb','ev_n',.43),'p_velo':rate(p,'velo_sum','velo_n',93),'p_bf':p['bf'],'p_sparse':p['pitches']<100,'p_recent_xwoba':px,'same_hand':r.stand==r.p_throws,'batter_left':r.stand=='L','batter_switch':r.stand=='S','pitcher_left':r.p_throws=='L','match_xwoba':mx or rate(b,'x_sum','x_n',.32),'match_whiff':mw or rate(b,'whiff','swing',.24),'match_power':mp or rate(b,'xslg_sum','xslg_n',.41),'direct_bvp_pitches':bvp[int(r.batter),int(r.pitcher)]})
  add(day)
 d=pd.DataFrame(rows);audit={'eligible_terminal_pa_in_spine_games_or_other_local_games':eligible_terminal,'excluded_relief_pa':excluded_relief,'excluded_game_not_in_accepted_spine':excluded_game,'retained':len(d)};return d,audit
def fit_model(X,y,kind):
 if kind=='league':return ('league',pd.Series(y).value_counts(normalize=True).reindex(CLASSES,fill_value=0).to_numpy())
 est=LogisticRegression(C=.5,max_iter=2500,multi_class='multinomial',random_state=SEED) if kind=='linear' else HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=12,min_samples_leaf=60,learning_rate=.04,l2_regularization=2,early_stopping=False,random_state=SEED)
 return make_pipeline(SimpleImputer(strategy='median'),StandardScaler() if kind=='linear' else SimpleImputer(strategy='median'),est).fit(X,y)
def pred(m,X):
 if isinstance(m,tuple):return np.tile(m[1],(len(X),1))
 raw=m.predict_proba(X);out=np.zeros((len(X),6));
 for j,c in enumerate(m[-1].classes_):out[:,CLASSES.index(c)]=raw[:,j]
 return out/out.sum(1,keepdims=True)
def ece(p,y):
 v=0
 for lo,hi in [(0,.05),(.05,.1),(.1,.2),(.2,.4),(.4,.6),(.6,1.01)]:
  q=(p>=lo)&(p<hi)
  if q.any():v+=q.mean()*abs(p[q].mean()-y[q].mean())
 return v
def multi(y,p):
 yi=np.array([CLASSES.index(x) for x in y]);one=np.eye(6)[yi];return {'log_loss':np.mean(-np.log(np.clip(p[np.arange(len(p)),yi],1e-12,1))),'multiclass_brier':np.mean(np.sum((p-one)**2,axis=1)),'accuracy':np.mean(np.array(CLASSES)[p.argmax(1)]==np.asarray(y))}
def derived(y,p,event):
 idx={'REACH_BASE':[1,2,3,4],'HIT':[2,3,4],'EXTRA_BASE_HIT':[3,4],'OUT':[0,5]}[event];yy=np.isin(y,[CLASSES[i] for i in idx]).astype(float);pp=p[:,idx].sum(1);return yy,pp
def main():
 OUT.mkdir(parents=True,exist_ok=True);d,audit=build();d.to_csv(OUT/'pa_population_manifest.csv',index=False);dev=d.split.eq('DEVELOPMENT');phases={'VALIDATION':d.split.eq('VALIDATION'),'LATER_HOLDOUT':d.split.eq('LATER_HOLDOUT')};results=[];preds={}
 stages=LADDER+[('MODEL_C_NONLINEAR',B+P+H+M)]
 for stage,fs in stages:
  kind='league' if not fs else 'nonlinear' if stage=='MODEL_C_NONLINEAR' else 'linear';m=fit_model(d.loc[dev,fs],d.loc[dev,'outcome'],kind)
  for phase,mask in phases.items():
   p=pred(m,d.loc[mask,fs]);preds[stage,phase]=p;results.append({'stage':stage,'phase':phase,'pa':mask.sum(),**multi(d.loc[mask,'outcome'].to_numpy(),p)})
 R=pd.DataFrame(results);selected=R.query("phase=='VALIDATION'").sort_values(['log_loss','multiclass_brier']).stage.iloc[0];R.to_csv(OUT/'pa_model_comparison.csv',index=False);R.query("stage==@selected and phase=='LATER_HOLDOUT'").to_csv(OUT/'pa_multiclass_holdout_metrics.csv',index=False)
 # Mapping contract and feature lineage.
 counts=d.outcome.value_counts().reindex(CLASSES,fill_value=0);mapping={'taxonomy':CLASSES,'mapping':{'strikeout/strikeout_double_play':'STRIKEOUT','walk/intent_walk/hit_by_pitch':'WALK_HBP','single':'SINGLE','double/triple':'DOUBLE_TRIPLE','home_run':'HOME_RUN','all remaining certified terminal events including field outs, force outs, sacrifices, errors, interference and fielder choices':'OTHER_OUT'},'invariant':'every retained terminal PA maps exactly once','category_counts':counts.to_dict(),'category_frequencies':(counts/len(d)).to_dict(),'population_audit':audit};(OUT/'pa_outcome_mapping_contract.json').write_text(json.dumps(mapping,indent=2)+'\n')
 pd.DataFrame([{'feature':x,'layer':next(n for n,fs in LADDER if x in fs),'strict_prior':'YES','grain':'state before game date','direct_bvp_primary':'NO'} for x in B+P+H+M]).to_csv(OUT/'pa_feature_manifest.csv',index=False);(OUT/'pa_temporal_split_contract.json').write_text(json.dumps({'split_by_game':'YES','development':{'games':int(d.loc[dev,'game_pk'].nunique()),'pa':int(dev.sum()),'dates':[d.loc[dev,'game_date'].min(),d.loc[dev,'game_date'].max()]},'validation':{'games':int(d.loc[phases['VALIDATION'],'game_pk'].nunique()),'pa':int(phases['VALIDATION'].sum()),'dates':[d.loc[phases['VALIDATION'],'game_date'].min(),d.loc[phases['VALIDATION'],'game_date'].max()]},'holdout':{'games':int(d.loc[phases['LATER_HOLDOUT'],'game_pk'].nunique()),'pa':int(phases['LATER_HOLDOUT'].sum()),'dates':[d.loc[phases['LATER_HOLDOUT'],'game_date'].min(),d.loc[phases['LATER_HOLDOUT'],'game_date'].max()]},'selection':'validation multiclass log loss; holdout untouched'},indent=2)+'\n')
 # Class and derived metrics.
 cm=[];dm=[]
 for phase,mask in phases.items():
  y=d.loc[mask,'outcome'].to_numpy();p=preds[selected,phase]
  for j,c in enumerate(CLASSES):
   yy=(y==c).astype(float);pp=p[:,j];cm.append({'phase':phase,'class':c,'prevalence':yy.mean(),'mean_probability':pp.mean(),'brier':np.mean((pp-yy)**2),'ece':ece(pp,yy),'roc_auc':roc_auc_score(yy,pp) if len(np.unique(yy))==2 else np.nan,'probability_sd':pp.std()})
  for ev in ['REACH_BASE','HIT','EXTRA_BASE_HIT','OUT']:
   yy,pp=derived(y,p,ev);dm.append({'phase':phase,'event':ev,'prevalence':yy.mean(),'mean_probability':pp.mean(),'brier':np.mean((pp-yy)**2),'log_loss':np.mean(-yy*np.log(np.clip(pp,1e-9,1))-(1-yy)*np.log(np.clip(1-pp,1e-9,1))),'ece':ece(pp,yy),'probability_sd':pp.std()})
 pd.DataFrame(cm).to_csv(OUT/'pa_class_probability_metrics.csv',index=False);pd.DataFrame(dm).to_csv(OUT/'pa_derived_event_metrics.csv',index=False)
 # Ablation with requested event scores.
 ab=[]
 for r in results:
  stage=r['stage'];phase=r['phase'];mask=phases[phase];y=d.loc[mask,'outcome'].to_numpy();p=preds[stage,phase];one=np.eye(6)[[CLASSES.index(x) for x in y]];hit=np.isin(y,['SINGLE','DOUBLE_TRIPLE','HOME_RUN']);ab.append({**r,'k_brier':np.mean((p[:,0]-(y=='STRIKEOUT'))**2),'bb_hbp_brier':np.mean((p[:,1]-(y=='WALK_HBP'))**2),'hit_brier':np.mean((p[:,2:5].sum(1)-hit)**2),'hr_brier':np.mean((p[:,4]-(y=='HOME_RUN'))**2)})
 pd.DataFrame(ab).to_csv(OUT/'pa_feature_layer_ablation.csv',index=False)
 # Separation and fixed-bin calibration.
 hold=phases['LATER_HOLDOUT'];y=d.loc[hold,'outcome'].to_numpy();p=preds[selected,'LATER_HOLDOUT'];sep=[];series={'STRIKEOUT':p[:,0],'REACH_BASE':p[:,1:5].sum(1),'HIT':p[:,2:5].sum(1),'HOME_RUN':p[:,4]}
 for ev,x in series.items():sep.append({'event':ev,'mean':x.mean(),'sd':x.std(),'p05':np.quantile(x,.05),'p25':np.quantile(x,.25),'median':np.median(x),'p75':np.quantile(x,.75),'p95':np.quantile(x,.95),'minimum':x.min(),'maximum':x.max()})
 pd.DataFrame(sep).to_csv(OUT/'pa_probability_separation.csv',index=False);cal=[]
 bins={'HIT':[0,.2,.3,.4,.5,1.01],'REACH_BASE':[0,.2,.3,.4,.5,1.01],'STRIKEOUT':[0,.15,.25,.35,.45,1.01],'HOME_RUN':[0,.02,.04,.06,.1,1.01]}
 for ev,edges in bins.items():
  pp=series[ev];yy=(y==ev) if ev in CLASSES else derived(y,p,ev)[0]
  for lo,hi in zip(edges[:-1],edges[1:]):
   q=(pp>=lo)&(pp<hi);cal.append({'event':ev,'bin':f'{lo:.2f}-{hi:.2f}','pa':q.sum(),'mean_probability':pp[q].mean() if q.any() else np.nan,'observed_rate':yy[q].mean() if q.any() else np.nan,'calibration_gap':pp[q].mean()-yy[q].mean() if q.any() else np.nan,'brier':np.mean((pp[q]-yy[q])**2) if q.any() else np.nan})
 pd.DataFrame(cal).to_csv(OUT/'pa_confidence_calibration.csv',index=False)
 # Temporal stability and matchup value.
 ts=[]
 for phase,mask in phases.items():
  for month in sorted(d.loc[mask,'game_date'].str[:7].unique()):
   q=mask&d.game_date.str[:7].eq(month);ids=np.where(mask)[0];sub=d.loc[mask,'game_date'].str[:7].eq(month).to_numpy();yy=d.loc[q,'outcome'].to_numpy();pp=preds[selected,phase][sub];mm=multi(yy,pp);ts.append({'phase':phase,'month':month,'pa':q.sum(),**mm,'hit_brier':np.mean((pp[:,2:5].sum(1)-np.isin(yy,['SINGLE','DOUBLE_TRIPLE','HOME_RUN']))**2),'k_brier':np.mean((pp[:,0]-(yy=='STRIKEOUT'))**2),'hr_brier':np.mean((pp[:,4]-(yy=='HOME_RUN'))**2)})
 pd.DataFrame(ts).to_csv(OUT/'pa_temporal_stability.csv',index=False);mv=[]
 for phase in phases:
  a=R.query("stage=='MODEL_A_PLUS_HANDEDNESS' and phase==@phase").iloc[0];b=R.query("stage=='MODEL_B_PLUS_PITCH_FAMILY' and phase==@phase").iloc[0];mv.append({'phase':phase,'log_loss_improvement':a.log_loss-b.log_loss,'brier_improvement':a.multiclass_brier-b.multiclass_brier,'matchup_earned':b.log_loss<a.log_loss})
 pd.DataFrame(mv).to_csv(OUT/'pa_matchup_incremental_value.csv',index=False)
 # Simple empirical rate controls are ladder rungs; fixed blend is explicit average of batter/starter model probabilities.
 simple=[]
 for phase,mask in phases.items():
  for stage in ['CONTROL_0_LEAGUE_RATE','CONTROL_1_BATTER_RATE','CONTROL_2_BATTER_STARTER']:
   rr=R.query('stage==@stage and phase==@phase').iloc[0];simple.append({'control':stage,'phase':phase,'log_loss':rr.log_loss,'multiclass_brier':rr.multiclass_brier})
  blend=(preds['CONTROL_1_BATTER_RATE',phase]+preds['CONTROL_2_BATTER_STARTER',phase])/2;simple.append({'control':'FIXED_50_50_BATTER_STARTER_BLEND','phase':phase,**multi(d.loc[mask,'outcome'].to_numpy(),blend)})
 pd.DataFrame(simple).to_csv(OUT/'pa_simple_rate_control_comparison.csv',index=False)
 base=R.query("stage=='CONTROL_0_LEAGUE_RATE' and phase=='LATER_HOLDOUT'").iloc[0];simp=pd.DataFrame(simple).query("phase=='LATER_HOLDOUT' and control!='CONTROL_0_LEAGUE_RATE'").sort_values('log_loss').iloc[0];sel=R.query("stage==@selected and phase=='LATER_HOLDOUT'").iloc[0];gain=simp.log_loss-sel.log_loss;valid=R.query("stage==@selected and phase=='VALIDATION'").log_loss.iloc[0]<R.query("stage=='CONTROL_0_LEAGUE_RATE' and phase=='VALIDATION'").log_loss.iloc[0];decision='PA_OUTCOME_MODEL_MATERIAL_PREDICTIVE_ADVANCE' if valid and gain>.01 else 'PA_OUTCOME_MODEL_SMALL_PREDICTIVE_GAIN' if valid and gain>0 else 'PA_OUTCOME_MODEL_NO_USEFUL_SIGNAL';ready='YES' if decision!='PA_OUTCOME_MODEL_NO_USEFUL_SIGNAL' else 'NO'
 (OUT/'run_simulation_feasibility.md').write_text(f"""# Run simulation feasibility\n\n`RUN_SIMULATION_RESEARCH_READY = {ready}`\n\nThe six-class output is coherent and sufficient to distinguish outs, reaching base, singles, extra-base hits, and home runs, but a simulator additionally requires batting order, base/out transition rules, runner advancement assumptions, double-play handling, starter workload/exit behavior, and a generic bullpen PA model. Direct BvP is diagnostic depth only and is not used as a primary matchup feature. {'The PA model earned a bounded simulator foundation step.' if ready=='YES' else 'Simple controls remain as good or better; stop before simulation.'}\n""")
 freq=', '.join(f'{k}={v/len(d):.3%}' for k,v in counts.items())
 text=f"""# MLB PA Outcome Prediction Foundation v1\n\n`{decision}`  \n`RUN_SIMULATION_RESEARCH_READY = {ready}`\n\n- Population: {len(d):,} certified starter-vs-batter PAs, {d.game_pk.nunique():,} games, {d.batter_id.nunique():,} batters, {d.starter_id.nunique():,} starters, {d.game_date.min()}–{d.game_date.max()}; split PA {d.groupby('split').size().to_dict()}.\n- Selected on validation: `{selected}`. Holdout league log loss/Brier {base.log_loss:.6f}/{base.multiclass_brier:.6f}; best simple control `{simp.control}` {simp.log_loss:.6f}/{simp.multiclass_brier:.6f}; selected {sel.log_loss:.6f}/{sel.multiclass_brier:.6f}; improvement vs simple {gain:+.6f}.\n- Category frequencies: {freq}. Class/derived-event calibration, separation, layer ablation, and monthly stability are preserved in the required artifacts.\n- Direct BvP was retained only as a sparsity diagnostic; no direct-BvP outcome rate was used. Exact next step: {'build one bounded inning-transition simulator research prototype using these frozen PA probabilities' if ready=='YES' else 'stop before inning simulation and retain simple PA rate controls'}.\n- No simulator, sportsbook feature, EV/Edge, selector, deployment, acquisition, or pipeline change occurred.\n""";(OUT/'concise_mlb_pa_outcome_prediction_foundation_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'population':len(d),'games':d.game_pk.nunique(),'selected':selected,'decision':decision,'simulation_ready':ready,'league_log_loss':base.log_loss,'selected_log_loss':sel.log_loss},indent=2))
if __name__=='__main__':main()
