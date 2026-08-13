#!/usr/bin/env python3
"""Bounded direct F5 ladder probability experiment on frozen feature state."""
from __future__ import annotations
import hashlib,json
from pathlib import Path
import numpy as np,pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from backend.mlb.scripts import run_mlb_expected_quality_scoring_model_v1 as eq
from backend.mlb.scripts import run_mlb_f5_expected_quality_distribution_refinement_v1 as dr

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_f5_direct_ladder_probability_model_v1/2026-08-12';SRC=ROOT/'artifacts/analysis/model_development/mlb_expected_quality_scoring_model_v1/2026-08-12/expected_quality_model_population.csv';MEANS=ROOT/'artifacts/analysis/model_development/mlb_f5_expected_quality_distribution_refinement_v1/2026-08-12/f5_frozen_mean_predictions.csv';DCT=ROOT/'artifacts/analysis/model_development/mlb_f5_expected_quality_distribution_refinement_v1/2026-08-12/distribution_candidate_contract.json';SEED=20260812
GAME_LINES=[3.5,4,4.5,5,5.5];TEAM_LINES=[1.5,2,2.5,3,3.5]
LAYERS=[('A_POPULATION_BASELINE',[]),('B_TEAM_STATE',eq.TEAM),('C_BATTER_EXPECTED',eq.TEAM+eq.BAT),('D_STARTER_EXPECTED',eq.TEAM+eq.BAT+eq.SP),('E_PITCH_FAMILY_MATCHUP',eq.TEAM+eq.BAT+eq.SP+eq.MATCH)]
MODELS=['CONTROL_A_V1_DISTRIBUTION','MODEL_B_DIRECT_LOGISTIC','MODEL_C_JOINT_LINE_LOGISTIC','MODEL_D_ORDINAL_CUMULATIVE','MODEL_E_SHALLOW_NONLINEAR']
def sh(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def ece(p,y):
 p=np.asarray(p);y=np.asarray(y);v=0
 for lo,hi in [(0,.2),(.2,.4),(.4,.6),(.6,.8),(.8,1.00001)]:
  q=(p>=lo)&(p<hi)
  if q.any():v+=q.mean()*abs(p[q].mean()-y[q].mean())
 return v
def logistic():return make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),LogisticRegression(C=.5,max_iter=2000,random_state=SEED))
def hgb():return make_pipeline(SimpleImputer(strategy='median'),HistGradientBoostingClassifier(max_iter=100,max_leaf_nodes=10,min_samples_leaf=40,learning_rate=.04,l2_regularization=2,early_stopping=False,random_state=SEED))
def fit_binary(X,y,kind='logistic'):
 if pd.Series(y).nunique()<2:return ('constant',float(np.mean(y)))
 return (logistic() if kind=='logistic' else hgb()).fit(X,y)
def pred_binary(m,X):return np.repeat(m[1],len(X)) if isinstance(m,tuple) else m.predict_proba(X)[:,list(m.classes_).index(1)]
def feature_cols(side=None,layer=None):
 fs=dict(LAYERS)[layer or 'E_PITCH_FAMILY_MATCHUP']
 if side:return [x if x in eq.TEAM else f'{side}_{x}' for x in fs]
 return [x for x in fs if x in eq.TEAM]+[f'{s}_{x}' for s in ['away','home'] for x in fs if x not in eq.TEAM]
def monotone(over):
 # Increasing thresholds cannot have higher unconditional Over probability.
 return np.minimum.accumulate(np.asarray(over),axis=1)
def fit_predict(name,d,train,test,score,lines,cols,control=None):
 Xtr=d.loc[train,cols];Xte=d.loc[test,cols];y=d.loc[train,score].astype(int);n=test.sum()
 if name=='CONTROL_A_V1_DISTRIBUTION':
  return control
 if name in ['MODEL_B_DIRECT_LOGISTIC','MODEL_E_SHALLOW_NONLINEAR']:
  kind='hgb' if name.endswith('NONLINEAR') else 'logistic';ovs=[];pus=[]
  for line in lines:
   ovs.append(pred_binary(fit_binary(Xtr,(y>line).astype(int),kind),Xte));pus.append(pred_binary(fit_binary(Xtr,(y==line).astype(int),kind),Xte) if float(line).is_integer() else np.zeros(n))
  over=monotone(np.array(ovs).T);push=np.array(pus).T
 elif name=='MODEL_C_JOINT_LINE_LOGISTIC':
  tx=pd.concat([Xtr.assign(_line=line) for line in lines],ignore_index=True);oy=np.concatenate([(y>line).astype(int) for line in lines]);py=np.concatenate([(y==line).astype(int) for line in lines]);om=fit_binary(tx,oy);pm=fit_binary(tx,py);ovs=[];pus=[]
  for line in lines:
   q=Xte.assign(_line=line);ovs.append(pred_binary(om,q));pus.append(pred_binary(pm,q) if float(line).is_integer() else np.zeros(n))
  over=monotone(np.array(ovs).T);push=np.array(pus).T
 else:
  # Direct ordered-outcome probabilities; cap only the response class, not its mean.
  cap=10;yc=np.minimum(y,cap);m=make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),LogisticRegression(C=.5,max_iter=2500,random_state=SEED)).fit(Xtr,yc);raw=m.predict_proba(Xte);classes=m[-1].classes_;over=[];push=[]
  for line in lines:
   over.append(raw[:,classes>line].sum(axis=1));push.append(raw[:,classes==line].sum(axis=1) if float(line).is_integer() else np.zeros(n))
  over=monotone(np.array(over).T);push=np.array(push).T
 push=np.minimum(push,np.maximum(0,1-over));under=np.minimum(np.maximum.accumulate(np.maximum(0,1-over-push),axis=1),1-over);push=np.maximum(0,1-over-under);norm=over+under+push;return over/norm,under/norm,push/norm
def control_probs(d,test,score,lines,side=None):
 pars=json.loads(DCT.read_text())['parameters'];out=[];under=[];push=[]
 for i in d.index[test]:
  if score=='f5_total':
   pa=dr.pmf(d.away_f5_expected_runs[i],pars['current_away_alpha']);ph=dr.pmf(d.home_f5_expected_runs[i],pars['current_home_alpha']);p=dr.conv([pa,ph])
  else:p=dr.pmf(d[f'{side}_f5_expected_runs'][i],pars[f'current_{side}_alpha'])
  k=np.arange(len(p));oo=[];uu=[];pp=[]
  for line in lines:oo.append(p[k>line].sum());pp.append(p[k==line].sum());uu.append(p[k<line].sum())
  out.append(oo);under.append(uu);push.append(pp)
 return np.array(out),np.array(under),np.array(push)
def rows_for(name,phase,market,ids,actual,lines,probs):
 over,under,push=probs;rows=[]
 for j,line in enumerate(lines):
  valid=actual!=line;y=(actual[valid]>line).astype(float);p=np.clip(over[valid,j]/np.maximum(1-push[valid,j],1e-9),1e-9,1-1e-9)
  rows.append({'model':name,'phase':phase,'market':market,'line':line,'games':len(actual),'resolved':valid.sum(),'pushes':int((actual==line).sum()),'brier':np.mean((p-y)**2),'log_loss':np.mean(-y*np.log(p)-(1-y)*np.log(1-p)),'ece':ece(p,y),'observed_over_rate':y.mean(),'mean_predicted_over':p.mean(),'probability_sd':p.std()})
 return rows
def run_family(d,score,lines,cols,market,side=None,selected_only=None):
 masks={x:d.split.eq(x) for x in ['DEVELOPMENT','VALIDATION','LATER_HOLDOUT']};rows=[];saved={}
 candidates=[selected_only] if selected_only else MODELS
 for name in candidates:
  for phase in ['VALIDATION','LATER_HOLDOUT']:
   ctl=control_probs(d,masks[phase],score,lines,side) if name=='CONTROL_A_V1_DISTRIBUTION' else None;pr=fit_predict(name,d,masks['DEVELOPMENT'],masks[phase],score,lines,cols,ctl);saved[name,phase]=pr;actual=d.loc[masks[phase],score].to_numpy();rows+=rows_for(name,phase,market,d.index[masks[phase]],actual,lines,pr)
 return pd.DataFrame(rows),saved
def pooled(r):return r.groupby(['model','phase','market']).agg(lines=('line','size'),brier=('brier','mean'),log_loss=('log_loss','mean'),ece=('ece','mean'),probability_sd=('probability_sd','mean')).reset_index()
def main():
 OUT.mkdir(parents=True,exist_ok=True);d=pd.read_csv(SRC);m=pd.read_csv(MEANS)[['game_pk','away_f5_expected_runs','home_f5_expected_runs','combined_f5_expected_runs']];d=d.merge(m,on='game_pk',validate='one_to_one');d['f5_total']=d.away_f5_runs+d.home_f5_runs
 # Primary fixed formulation comparison.
 gm,gpred=run_family(d,'f5_total',GAME_LINES,feature_cols(),'F5_GAME_TOTAL');P=pooled(gm);overall_winner=P.query("phase=='VALIDATION'").sort_values(['brier','log_loss','ece']).model.iloc[0];selected=P.query("phase=='VALIDATION' and model!='CONTROL_A_V1_DISTRIBUTION'").sort_values(['brier','log_loss','ece']).model.iloc[0]
 # Same selected formulation for both team sides; control retained for exact comparison.
 team=[];tp={}
 for side in ['away','home']:
  q,s=run_family(d,f'{side}_f5_runs',TEAM_LINES,feature_cols(side),'F5_'+side.upper()+'_TEAM_TOTAL',side,selected_only=selected);c,cs=run_family(d,f'{side}_f5_runs',TEAM_LINES,feature_cols(side),'F5_'+side.upper()+'_TEAM_TOTAL',side,selected_only='CONTROL_A_V1_DISTRIBUTION');team.extend([q,c]);tp.update({(side,k):v for k,v in {**s,**cs}.items()})
 TM=pd.concat(team,ignore_index=True);pd.concat([P,pooled(TM)],ignore_index=True).to_csv(OUT/'direct_probability_model_comparison.csv',index=False);gm.to_csv(OUT/'f5_game_total_ladder_metrics.csv',index=False);TM.to_csv(OUT/'f5_team_total_ladder_metrics.csv',index=False)
 # Population ledger includes selected frozen probabilities.
 pop=d[['game_pk','date','start','split','away_f5_runs','home_f5_runs','f5_total','away_f5_expected_runs','home_f5_expected_runs','combined_f5_expected_runs']].copy()
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  idx=np.where(d.split.eq(phase))[0];pr=gpred[selected,phase]
  for j,line in enumerate(GAME_LINES):pop.loc[idx,f'p_over_{str(line).replace(".","_")}']=pr[0][:,j];pop.loc[idx,f'p_push_{str(line).replace(".","_")}']=pr[2][:,j]
 pop.to_csv(OUT/'direct_probability_population.csv',index=False)
 contract={'population':1594,'splits':d.groupby('split').size().to_dict(),'features':'frozen team + batter expected + starter expected + shrunk pitch-family matchup','models':MODELS,'selection':'overall winner and best direct challenger ranked separately on validation pooled game-total Brier, then log loss, then ECE; holdout untouched','validation_overall_winner':overall_winner,'validation_best_direct_challenger':selected,'whole_line':'unconditional direct Over and Push; Under=1-Over-Push; conditional no-push evaluation renormalizes','monotonic_correction':'row-wise cumulative minimum across increasing lines','input_hashes':{str(SRC.relative_to(ROOT)):sh(SRC),str(MEANS.relative_to(ROOT)):sh(MEANS)}};(OUT/'direct_probability_feature_contract.json').write_text(json.dumps(contract,indent=2)+'\n')
 # Coherence.
 coh=[]
 for name in MODELS:
  for phase in ['VALIDATION','LATER_HOLDOUT']:
   o,u,p=gpred[name,phase];coh.append({'model':name,'phase':phase,'rows':len(o),'over_monotonic_violations':int((np.diff(o,axis=1)>1e-12).any(axis=1).sum()),'under_monotonic_violations':int((np.diff(u,axis=1)<-1e-12).any(axis=1).sum()),'range_violations':int(((o<0)|(o>1)|(u<0)|(u>1)|(p<0)|(p>1)).any(axis=1).sum()),'sum_to_one_max_abs_error':np.max(abs(o+u+p-1))})
 pd.DataFrame(coh).to_csv(OUT/'ladder_coherence_validation.csv',index=False)
 # One fixed expected-quality ablation using direct regularized logistic.
 ab=[]
 for layer,fs in LAYERS:
  cols=feature_cols(layer=layer)
  if not cols:
   # line/population baseline: one empirical development rate per line.
   for phase in ['VALIDATION','LATER_HOLDOUT']:
    rows=[]
    for line in GAME_LINES:
     tr=d.split.eq('DEVELOPMENT');te=d.split.eq(phase);prob=np.repeat((d.loc[tr,'f5_total']>line).mean(),te.sum());y=(d.loc[te,'f5_total']>line).astype(float);rows.append((np.mean((prob-y)**2),np.mean(-y*np.log(prob)-(1-y)*np.log(1-prob)),ece(prob,y)))
    ab.append({'layer':layer,'phase':phase,'brier':np.mean([x[0] for x in rows]),'log_loss':np.mean([x[1] for x in rows]),'ece':np.mean([x[2] for x in rows])})
  else:
   q,_=run_family(d,'f5_total',GAME_LINES,cols,'F5_GAME_TOTAL',selected_only='MODEL_B_DIRECT_LOGISTIC');w=pooled(q)
   for x in w.itertuples():ab.append({'layer':layer,'phase':x.phase,'brier':x.brier,'log_loss':x.log_loss,'ece':x.ece})
 pd.DataFrame(ab).to_csv(OUT/'expected_quality_direct_probability_ablation.csv',index=False)
 # Selected confidence calibration, favored side only and no duplicated complement evidence.
 conf=[]
 for family,frame in [('F5_GAME_TOTAL',gm),('F5_TEAM_TOTAL',TM)]:
  specs=[]
  if family=='F5_GAME_TOTAL':
   for phase in ['VALIDATION','LATER_HOLDOUT']:
    o,u,p=gpred[selected,phase];act=d.loc[d.split.eq(phase),'f5_total'].to_numpy()
    for j,line in enumerate(GAME_LINES):
     valid=act!=line;po=o[valid,j]/(1-p[valid,j]);fav=np.maximum(po,1-po);success=np.where(po>=.5,act[valid]>line,act[valid]<line);specs.extend(zip([phase]*len(fav),fav,success))
  else:
   for side in ['away','home']:
    for phase in ['VALIDATION','LATER_HOLDOUT']:
     o,u,p=tp[side,(selected,phase)];act=d.loc[d.split.eq(phase),f'{side}_f5_runs'].to_numpy()
     for j,line in enumerate(TEAM_LINES):
      valid=act!=line;po=o[valid,j]/(1-p[valid,j]);fav=np.maximum(po,1-po);success=np.where(po>=.5,act[valid]>line,act[valid]<line);specs.extend(zip([phase]*len(fav),fav,success))
  for phase in ['VALIDATION','LATER_HOLDOUT']:
   vals=[x for x in specs if x[0]==phase]
   for lo,hi,label in [(.5,.55,'50_54_99'),(.55,.6,'55_59_99'),(.6,.65,'60_64_99'),(.65,.7,'65_69_99'),(.7,1.01,'GE_70')]:
    q=[x for x in vals if lo<=x[1]<hi];conf.append({'family':family,'phase':phase,'confidence_bin':label,'predictions':len(q),'mean_probability':np.mean([x[1] for x in q]) if q else np.nan,'observed_success':np.mean([x[2] for x in q]) if q else np.nan,'calibration_gap':np.mean([x[1]-x[2] for x in q]) if q else np.nan,'brier':np.mean([(x[1]-x[2])**2 for x in q]) if q else np.nan})
 pd.DataFrame(conf).to_csv(OUT/'confidence_calibration.csv',index=False)
 # Temporal stability (June validation / July holdout are exact months).
 temp=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  q=gm.query('model==@selected and phase==@phase');temp.append({'slice_type':'SPLIT_MONTH','slice_value':d.loc[d.split.eq(phase),'date'].str[:7].iloc[0],'phase':phase,'market':'F5_GAME_TOTAL','brier':q.brier.mean(),'log_loss':q.log_loss.mean(),'ece':q.ece.mean(),'probability_sd':q.probability_sd.mean()})
  q=TM.query('model==@selected and phase==@phase');temp.append({'slice_type':'SPLIT_MONTH','slice_value':d.loc[d.split.eq(phase),'date'].str[:7].iloc[0],'phase':phase,'market':'F5_TEAM_TOTAL','brier':q.brier.mean(),'log_loss':q.log_loss.mean(),'ece':q.ece.mean(),'probability_sd':q.probability_sd.mean()})
 pd.DataFrame(temp).to_csv(OUT/'temporal_stability.csv',index=False)
 # Exact V1 vs selected comparison per line and pooled.
 cmp=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  a=gm.query("model=='CONTROL_A_V1_DISTRIBUTION' and phase==@phase").set_index('line');b=gm.query("model==@selected and phase==@phase").set_index('line')
  for line in GAME_LINES:cmp.append({'phase':phase,'market':'F5_GAME_TOTAL','line':line,'v1_brier':a.loc[line].brier,'direct_brier':b.loc[line].brier,'brier_delta_direct_minus_v1':b.loc[line].brier-a.loc[line].brier,'v1_log_loss':a.loc[line].log_loss,'direct_log_loss':b.loc[line].log_loss,'log_loss_delta':b.loc[line].log_loss-a.loc[line].log_loss,'ece_delta':b.loc[line].ece-a.loc[line].ece,'probability_sd_delta':b.loc[line].probability_sd-a.loc[line].probability_sd})
 pd.DataFrame(cmp).to_csv(OUT/'v1_distribution_vs_direct_probability.csv',index=False)
 c=pd.DataFrame(cmp).query("phase=='LATER_HOLDOUT'");v1=c.v1_brier.mean();direct=c.direct_brier.mean();material=[{'metric':'absolute pooled Brier improvement','value':v1-direct},{'metric':'relative Brier improvement percent','value':100*(v1-direct)/v1},{'metric':'pooled log-loss improvement','value':(c.v1_log_loss-c.direct_log_loss).mean()},{'metric':'ECE change direct-minus-V1','value':c.ece_delta.mean()},{'metric':'lines improved','value':int((c.brier_delta_direct_minus_v1<0).sum())},{'metric':'lines worsened','value':int((c.brier_delta_direct_minus_v1>0).sum())}];pd.DataFrame(material).to_csv(OUT/'direct_probability_materiality.csv',index=False)
 val=P.query("phase=='VALIDATION'").set_index('model');hold=P.query("phase=='LATER_HOLDOUT'").set_index('model');improve=hold.loc['CONTROL_A_V1_DISTRIBUTION','brier']-hold.loc[selected,'brier'];valid=val.loc[selected,'brier']<val.loc['CONTROL_A_V1_DISTRIBUTION','brier'];ll=hold.loc[selected,'log_loss']<hold.loc['CONTROL_A_V1_DISTRIBUTION','log_loss'];cal=hold.loc[selected,'ece']<=hold.loc['CONTROL_A_V1_DISTRIBUTION','ece']+.005;decision='DIRECT_LADDER_PROBABILITY_MATERIAL_ADVANCE' if valid and improve>.005 and ll and cal else 'DIRECT_LADDER_PROBABILITY_SMALL_IMPROVEMENT' if valid and improve>0 and ll and cal else 'DIRECT_LADDER_PROBABILITY_NO_IMPROVEMENT';gready='READY' if decision.endswith('MATERIAL_ADVANCE') else 'BELOW_BAR' if improve>=0 else 'NOT_READY';tpool=pooled(TM);tv=tpool.query("model=='CONTROL_A_V1_DISTRIBUTION' and phase=='LATER_HOLDOUT'").brier.mean();td=tpool.query("model==@selected and phase=='LATER_HOLDOUT'").brier.mean();tready='READY' if decision.endswith('MATERIAL_ADVANCE') and td<tv else 'BELOW_BAR' if td<=tv else 'NOT_READY';pd.DataFrame([{'family':'F5_GAME_TOTAL','declaration':gready},{'family':'F5_TEAM_TOTAL','declaration':tready}]).to_csv(OUT/'market_family_readiness.csv',index=False)
 strongest=c.sort_values('brier_delta_direct_minus_v1').iloc[0];weakest=c.sort_values('brier_delta_direct_minus_v1').iloc[-1];abl=pd.DataFrame(ab).query("phase=='LATER_HOLDOUT'").set_index('layer');text=f"""# MLB F5 Direct Ladder Probability Model v1\n\n`{decision}`\n\n- Population 1,594; development/validation/untouched holdout 886/394/314. Selected on validation: `{selected}`.\n- Holdout pooled V1 Brier/log loss/ECE {hold.loc['CONTROL_A_V1_DISTRIBUTION','brier']:.6f}/{hold.loc['CONTROL_A_V1_DISTRIBUTION','log_loss']:.6f}/{hold.loc['CONTROL_A_V1_DISTRIBUTION','ece']:.6f}; direct {hold.loc[selected,'brier']:.6f}/{hold.loc[selected,'log_loss']:.6f}/{hold.loc[selected,'ece']:.6f}. Brier improvement {improve:+.6f} ({100*improve/hold.loc['CONTROL_A_V1_DISTRIBUTION','brier']:+.3f}%).\n- Strongest/weakest holdout lines by Brier delta: {strongest.line}/{weakest.line}. Fixed-layer holdout Brier: team {abl.loc['B_TEAM_STATE','brier']:.6f}, +batter {abl.loc['C_BATTER_EXPECTED','brier']:.6f}, +starter {abl.loc['D_STARTER_EXPECTED','brier']:.6f}, +pitch family {abl.loc['E_PITCH_FAMILY_MATCHUP','brier']:.6f}.\n- Confidence calibration, probability separation (per-line probability SD), team totals, and June/July stability are in the accompanying artifacts. Team-total pooled Brier V1/direct: {tv:.6f}/{td:.6f}.\n- Readiness: `F5_GAME_TOTAL={gready}`; `F5_TEAM_TOTAL={tready}`. Exact next step: {'freeze this direct formulation for an independent prospective probability audit' if decision!='DIRECT_LADDER_PROBABILITY_NO_IMPROVEMENT' else 'retain the V1 distribution probabilities and stop direct-ladder research'}. No sportsbook data, EV/Edge, selector, deployment, acquisition, or pipeline change occurred.\n""";(OUT/'concise_mlb_f5_direct_ladder_probability_model_v1.md').write_text(text)
 text=text.replace(f'Selected on validation: `{selected}`.',f'Validation overall winner: `{overall_winner}`; best direct challenger: `{selected}`.');(OUT/'concise_mlb_f5_direct_ladder_probability_model_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files));print(json.dumps({'overall_winner':overall_winner,'best_direct_challenger':selected,'decision':decision,'v1_brier':hold.loc['CONTROL_A_V1_DISTRIBUTION','brier'],'direct_brier':hold.loc[selected,'brier'],'team_v1_brier':tv,'team_direct_brier':td},indent=2))
if __name__=='__main__':main()
