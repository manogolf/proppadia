#!/usr/bin/env python3
"""Transparent, bounded starter-led MLB scoring prediction foundation."""
from __future__ import annotations
import hashlib,json
from collections import defaultdict,deque
from pathlib import Path
import numpy as np,pandas as pd
from scipy.stats import poisson
from sklearn.impute import SimpleImputer
from sklearn.linear_model import PoissonRegressor,LinearRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from backend.mlb.scripts import run_mlb_lineup_confirmed_scoring_prediction_v2 as v2

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_starter_led_scoring_prediction_foundation_v1/2026-08-12';CAP=30
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def pmf(mu):
 q=poisson.pmf(np.arange(CAP+1),max(mu,.02));q[-1]+=max(0,1-q.sum());return q/q.sum()
def conv(ps):
 q=np.array([1.])
 for p in ps:q=np.convolve(q,p)
 q=q[:CAP+1];q[-1]+=max(0,1-q.sum());return q/q.sum()
def crps(p,y):
 k=np.arange(len(p));return np.sum((np.cumsum(p)-(k>=int(y)))**2)
def metrics(y,mu,ps=None):return {'games':len(y),'mae':np.mean(abs(mu-y)),'rmse':np.mean((mu-y)**2)**.5,'bias':np.mean(mu-y),'crps':np.mean([crps(pmf(a) if ps is None else p,b) for a,b,p in zip(mu,y,ps or [None]*len(y))]),'prediction_sd':np.std(mu)}

def population():
 games=v2.parse();d=v2.features(games);state=defaultdict(lambda:{'outs':0,'r':0,'er':0,'h':0,'bb':0,'k':0,'hr':0,'starts':0,'outs_recent':deque(maxlen=5),'ra_recent3':deque(maxlen=3),'ra_recent5':deque(maxlen=5)});extra=[]
 for g in games:
  r={'game_pk':g['game_pk']}
  for side in ['away','home']:
   pid,st=g['starters'][side];s=state[pid];bf=s['outs']+s['h']+s['bb'];r[f'{side}_starter_era']=27*s['er']/s['outs'] if s['outs'] else 4.5;r[f'{side}_starter_ra9_audit']=27*s['r']/s['outs'] if s['outs'] else 4.5;r[f'{side}_starter_whip']=3*(s['h']+s['bb'])/s['outs'] if s['outs'] else 1.3;r[f'{side}_starter_fip_like']=((13*s['hr']+3*s['bb']-2*s['k'])*3/s['outs']+3.2) if s['outs'] else 4.5;r[f'{side}_starter_recent3_ra9']=np.mean(s['ra_recent3']) if s['ra_recent3'] else 4.5;r[f'{side}_starter_recent5_ra9']=np.mean(s['ra_recent5']) if s['ra_recent5'] else 4.5;r[f'{side}_expected_outs_governed']=np.mean(s['outs_recent']) if s['outs_recent'] else (s['outs']/s['starts'] if s['starts'] else 15);r[f'{side}_workload_tier']='RECENT_5' if len(s['outs_recent'])>=3 else 'SEASON' if s['starts'] else 'ROLE_FALLBACK';r[f'{side}_actual_starter_outs']=int(st.get('outs',0));r[f'{side}_actual_starter_runs']=int(st.get('runs',0));r[f'{side}_actual_starter_er']=int(st.get('earnedRuns',st.get('runs',0)))
  extra.append(r)
  for side in ['away','home']:
   pid,st=g['starters'][side];s=state[pid];outs=int(st.get('outs',0));runs=int(st.get('runs',0));s['outs']+=outs;s['r']+=runs;s['er']+=int(st.get('earnedRuns',runs));s['h']+=int(st.get('hits',0));s['bb']+=int(st.get('baseOnBalls',0));s['k']+=int(st.get('strikeOuts',0));s['hr']+=int(st.get('homeRuns',0));s['starts']+=1;s['outs_recent'].append(outs);ra=27*runs/max(outs,1);s['ra_recent3'].append(ra);s['ra_recent5'].append(ra)
 d=d.merge(pd.DataFrame(extra),on='game_pk',validate='one_to_one');return d

def formulas(d):
 league_team=d.league_rpg_prior/2
 out={}
 # Opponent's pitcher determines a batting side's F5 expectation.
 for bat,pit in [('away','home'),('home','away')]:
  outs=np.clip(d[f'{pit}_expected_outs_governed'],9,21);remain=np.maximum(0,15-outs)
  era=d[f'{pit}_starter_era'];blend=.5*era+.3*d[f'{pit}_starter_ra9_audit']+.2*d[f'{pit}_starter_fip_like']
  a=np.repeat(d[[f'{bat}_f5_runs']].mean().iloc[0],len(d));b=era*outs/27+league_team*remain/27;c=blend*outs/27+league_team*remain/27;off=np.clip(d[f'{bat}_team_runs_prior']/league_team,.65,1.4);dd=c*off;e=dd*np.sqrt(np.clip(d.park_run_factor_prior,.7,1.35));
  for name,x in [('A_CONSTANT',a),('B_ERA_WORKLOAD',b),('C_BLEND_WORKLOAD',c),('D_STARTER_PLUS_OFFENSE',dd),('E_PLUS_PARK_ENV',e)]:out[name,bat]=np.maximum(.05,np.asarray(x))
 # Full game: frozen F5 candidates plus transparent post-F5 team bullpen prevention.
 for name in ['A_CONSTANT','B_ERA_WORKLOAD','C_BLEND_WORKLOAD','D_STARTER_PLUS_OFFENSE','E_PLUS_PARK_ENV']:
  for bat,fielding in [('away','home'),('home','away')]:
   post=np.repeat(d[f'{bat}_post_f5_runs'].mean(),len(d))
   if name in ['D_STARTER_PLUS_OFFENSE','E_PLUS_PARK_ENV']:
    off=np.clip(d[f'{bat}_team_runs_prior']/league_team,.65,1.4);post=np.maximum(.05,d[f'{fielding}_bullpen_ra']*off)
    if name=='E_PLUS_PARK_ENV':post*=np.sqrt(np.clip(d.park_run_factor_prior,.7,1.35))
   out[name,bat+'_post']=np.asarray(post)
 out['F_STARTER_OFFENSE_BULLPEN','away']=out['E_PLUS_PARK_ENV','away'];out['F_STARTER_OFFENSE_BULLPEN','home']=out['E_PLUS_PARK_ENV','home'];out['F_STARTER_OFFENSE_BULLPEN','away_post']=np.maximum(.05,d.home_bullpen_ra*np.clip(d.away_team_runs_prior/league_team,.65,1.4)*np.sqrt(np.clip(d.park_run_factor_prior,.7,1.35)));out['F_STARTER_OFFENSE_BULLPEN','home_post']=np.maximum(.05,d.away_bullpen_ra*np.clip(d.home_team_runs_prior/league_team,.65,1.4)*np.sqrt(np.clip(d.park_run_factor_prior,.7,1.35)))
 return out

def audit(d):
 rows=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  z=d[d.split==phase]
  for side,pit in [('away','home'),('home','away')]:
   for f in ['starter_era','starter_ra9_audit','starter_whip','starter_fip_like','starter_recent3_ra9','starter_recent5_ra9','expected_outs_governed']:
    x=z[f'{pit}_{f}'];
    for target in [f'{side}_f5_runs',f'{side}_full_runs']:
     y=z[target];rows.append({'phase':phase,'pitcher_side':pit,'feature':f,'target':target,'rows':len(z),'correlation':x.corr(y),'r_squared_simple':LinearRegression().fit(x.to_numpy().reshape(-1,1),y).score(x.to_numpy().reshape(-1,1),y)})
 pd.DataFrame(rows).to_csv(OUT/'starter_signal_audit.csv',index=False)
 w=[]
 for phase,g in d.groupby('split'):
  for side in ['away','home']:
   y=g[f'{side}_actual_starter_outs'];mu=g[f'{side}_expected_outs_governed'];w.append({'phase':phase,'side':side,**metrics(y.to_numpy(),mu.to_numpy())})
 pd.DataFrame(w).to_csv(OUT/'starter_workload_metrics.csv',index=False)

def choose(d,F):
 val=d.split.eq('VALIDATION');hold=d.split.eq('LATER_HOLDOUT');stages=['A_CONSTANT','B_ERA_WORKLOAD','C_BLEND_WORKLOAD','D_STARTER_PLUS_OFFENSE','E_PLUS_PARK_ENV'];lad=[]
 for name in stages:
  for phase,mask in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
   ya=d.loc[mask,'away_f5_runs'].to_numpy();yh=d.loc[mask,'home_f5_runs'].to_numpy();ma=F[name,'away'][mask];mh=F[name,'home'][mask];yt=ya+yh;mt=ma+mh
   for market,y,mu in [('AWAY_F5',ya,ma),('HOME_F5',yh,mh),('F5_TOTAL',yt,mt)]:lad.append({'stage':name,'phase':phase,'market':market,**metrics(y,mu)})
 L=pd.DataFrame(lad);L.to_csv(OUT/'progressive_baseline_ladder.csv',index=False);score=L[(L.phase=='VALIDATION')&(L.market=='F5_TOTAL')].set_index('stage').crps;selected_f5=score.idxmin()
 # Freeze F5, then select post extension using validation full-total CRPS.
 full=[]
 for name in stages+['F_STARTER_OFFENSE_BULLPEN']:
  f5name=selected_f5 if name=='F_STARTER_OFFENSE_BULLPEN' else name
  for phase,mask in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
   ma=F[f5name,'away'][mask]+F[name,'away_post'][mask];mh=F[f5name,'home'][mask]+F[name,'home_post'][mask];ya=d.loc[mask,'away_full_runs'].to_numpy();yh=d.loc[mask,'home_full_runs'].to_numpy();
   for market,y,mu in [('AWAY_FULL',ya,ma),('HOME_FULL',yh,mh),('FULL_TOTAL',ya+yh,ma+mh)]:full.append({'stage':name,'frozen_f5_stage':selected_f5,'phase':phase,'market':market,**metrics(y,mu)})
 G=pd.DataFrame(full);score2=G[(G.phase=='VALIDATION')&(G.market=='FULL_TOTAL')].set_index('stage').crps;selected_full=score2.idxmin();return L,G,selected_f5,selected_full,val,hold

def outputs(d,F,L,G,sf,sg,val,hold):
 L[(L.stage==sf)].to_csv(OUT/'f5_prediction_metrics.csv',index=False);G[(G.stage==sg)].to_csv(OUT/'full_game_prediction_metrics.csv',index=False)
 # Compact starter-rate comparison incl regularized pitcher-only model.
 rows=[];fs=['home_starter_era','home_starter_ra9_audit','home_starter_whip','home_starter_fip_like','home_starter_recent3_ra9','home_expected_outs_governed'];dev=d.split.eq('DEVELOPMENT')
 for phase,train,test in [('VALIDATION',dev,val),('LATER_HOLDOUT',dev|val,hold)]:
  m=make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),PoissonRegressor(alpha=1,max_iter=1000)).fit(d.loc[train,fs],d.loc[train,'away_f5_runs']);mu=np.maximum(.05,m.predict(d.loc[test,fs]));rows.append({'model':'CONTROL_C_REGULARIZED_STARTER_RATE','phase':phase,**metrics(d.loc[test,'away_f5_runs'].to_numpy(),mu)})
  for name in ['B_ERA_WORKLOAD','C_BLEND_WORKLOAD']:rows.append({'model':name,'phase':phase,**metrics(d.loc[test,'away_f5_runs'].to_numpy(),F[name,'away'][test])})
 pd.DataFrame(rows).to_csv(OUT/'starter_rate_model_comparison.csv',index=False)
 L[L.stage.isin(['C_BLEND_WORKLOAD','D_STARTER_PLUS_OFFENSE'])].to_csv(OUT/'offense_adjustment_comparison.csv',index=False)
 post=[]
 for phase,mask in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  for side in ['away','home']:
   y=d.loc[mask,f'{side}_post_f5_runs'].to_numpy();mu=F[sg,f'{side}_post'][mask];post.append({'stage':sg,'phase':phase,'component':f'{side.upper()}_POST_F5',**metrics(y,mu)})
 pd.DataFrame(post).to_csv(OUT/'post_f5_prediction_metrics.csv',index=False)
 # Existing models: exact compatible control values; other artifacts have incompatible populations.
 cmp=[]
 for label,stage in [('NEGATIVE_BINOMIAL_CONSTANT_CONTROL','A_CONSTANT'),('LINEUP_CONFIRMED_V2_SELECTED_CONTROL','A_CONSTANT'),('SELECTED_STARTER_LED',sf)]:
  r=L[(L.stage==stage)&(L.phase=='LATER_HOLDOUT')&L.market.eq('F5_TOTAL')].iloc[0];cmp.append({'model':label,'market':'F5_TOTAL','compatible_games':r.games,'mae':r.mae,'bias':r.bias,'crps':r.crps,'prediction_sd':r.prediction_sd,'comparison_status':'EXACT_COMMON_HOLDOUT'})
 for label,stage in [('NEGATIVE_BINOMIAL_CONSTANT_CONTROL','A_CONSTANT'),('LINEUP_CONFIRMED_V2_SELECTED_CONTROL','A_CONSTANT'),('SELECTED_STARTER_LED',sg)]:
  r=G[(G.stage==stage)&(G.phase=='LATER_HOLDOUT')&G.market.eq('FULL_TOTAL')].iloc[0];cmp.append({'model':label,'market':'FULL_TOTAL','compatible_games':r.games,'mae':r.mae,'bias':r.bias,'crps':r.crps,'prediction_sd':r.prediction_sd,'comparison_status':'EXACT_COMMON_HOLDOUT'})
 for label,note in [('FROZEN_TOTALS_V1','different retained 79-game compatible holdout; MAE 3.9824'),('PRIOR_DECOMPOSED_SCORING','different retained 79-game compatible holdout; MAE 3.9891')]:cmp.append({'model':label,'market':'FULL_TOTAL','comparison_status':note})
 for label,stage in [('NEGATIVE_BINOMIAL_CONSTANT_CONTROL','A_CONSTANT'),('SELECTED_STARTER_LED',sg)]:
  for market in ['AWAY_FULL','HOME_FULL']:
   r=G[(G.stage==stage)&(G.phase=='LATER_HOLDOUT')&G.market.eq(market)].iloc[0];cmp.append({'model':label,'market':market,'compatible_games':r.games,'mae':r.mae,'bias':r.bias,'crps':r.crps,'prediction_sd':r.prediction_sd,'comparison_status':'EXACT_COMMON_HOLDOUT'})
 pd.DataFrame(cmp).to_csv(OUT/'existing_model_comparison.csv',index=False)
 # ERA alignment and low/mid/high examples.
 er=[]
 for phase,mask in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  combo=(d.loc[mask,'away_starter_era']+d.loc[mask,'home_starter_era'])/2;pred=F[sf,'away'][mask]+F[sf,'home'][mask];actual=d.loc[mask,['away_f5_runs','home_f5_runs']].sum(axis=1)
  er.append({'phase':phase,'slice':'ALL','games':mask.sum(),'era_prediction_correlation':combo.corr(pd.Series(pred,index=combo.index)),'era_actual_correlation':combo.corr(actual),'era_only_r_squared':LinearRegression().fit(combo.to_numpy().reshape(-1,1),actual).score(combo.to_numpy().reshape(-1,1),actual),'partial_contribution_interpretation':'starter ERA contribution retained' if sf!='A_CONSTANT' else 'ERA did not earn selection'})
  bands=pd.qcut(combo,3,labels=['LOW','MID','HIGH'])
  for b in bands.unique():q=bands==b;er.append({'phase':phase,'slice':b,'games':q.sum(),'mean_combined_era':combo[q].mean(),'mean_predicted_f5':np.asarray(pred)[q].mean(),'mean_actual_f5':actual[q].mean()})
 pd.DataFrame(er).to_csv(OUT/'starter_era_alignment.csv',index=False)
 # Ladders and dispersion.
 h=d.loc[hold].copy();means={'F5_TOTAL':F[sf,'away'][hold]+F[sf,'home'][hold],'FULL_TOTAL':F[sf,'away'][hold]+F[sf,'home'][hold]+F[sg,'away_post'][hold]+F[sg,'home_post'][hold],'AWAY_FULL':F[sf,'away'][hold]+F[sg,'away_post'][hold],'HOME_FULL':F[sf,'home'][hold]+F[sg,'home_post'][hold]};actual={'F5_TOTAL':h.away_f5_runs+h.home_f5_runs,'FULL_TOTAL':h.away_full_runs+h.home_full_runs,'AWAY_FULL':h.away_full_runs,'HOME_FULL':h.home_full_runs};lines={'F5_TOTAL':[3.5,4,4.5,5,5.5],'FULL_TOTAL':[7.5,8,8.5,9,9.5,10],'AWAY_FULL':[2.5,3,3.5,4,4.5,5],'HOME_FULL':[2.5,3,3.5,4,4.5,5]};lr=[]
 for m,ls in lines.items():
  for line in ls:
   pp=[];yy=[];push=0
   for mu,y in zip(means[m],actual[m]):
    p=pmf(mu);k=np.arange(len(p));pu=p[k==line].sum();push+=y==line
    if y!=line:pp.append(np.clip(p[k>line].sum()/(1-pu),1e-8,1-1e-8));yy.append(y>line)
   pp=np.array(pp);yy=np.array(yy,float);lr.append({'market':m,'line':line,'resolved':len(yy),'pushes':push,'brier':np.mean((pp-yy)**2),'log_loss':np.mean(-yy*np.log(pp)-(1-yy)*np.log(1-pp)),'calibration_bias':pp.mean()-yy.mean(),'probability_sd':pp.std()})
 pd.DataFrame(lr).to_csv(OUT/'probability_ladder_calibration.csv',index=False)
 dr=[]
 for m,mu in means.items():
  avg=np.mean(mu);dr.append({'market':m,'mean':avg,'sd':np.std(mu),'minimum':np.min(mu),'p05':np.quantile(mu,.05),'p95':np.quantile(mu,.95),'maximum':np.max(mu),'pct_abs_from_mean_ge_0_5':np.mean(abs(mu-avg)>=.5),'pct_ge_1_0':np.mean(abs(mu-avg)>=1),'pct_ge_1_5':np.mean(abs(mu-avg)>=1.5)})
 pd.DataFrame(dr).to_csv(OUT/'prediction_dispersion.csv',index=False)
 ts=[]
 for phase,mask in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
  for m,y,mu in [('F5_TOTAL',d.loc[mask,['away_f5_runs','home_f5_runs']].sum(axis=1).to_numpy(),F[sf,'away'][mask]+F[sf,'home'][mask]),('FULL_TOTAL',d.loc[mask,['away_full_runs','home_full_runs']].sum(axis=1).to_numpy(),F[sf,'away'][mask]+F[sf,'home'][mask]+F[sg,'away_post'][mask]+F[sg,'home_post'][mask])]:ts.append({'slice_type':'SPLIT','slice_value':phase,'market':m,**metrics(y,mu)})
 for month in sorted(d.loc[val|hold,'date'].str[:7].unique()):
  mask=(d.date.str[:7]==month)&(val|hold)
  for m,y,mu in [('F5_TOTAL',d.loc[mask,['away_f5_runs','home_f5_runs']].sum(axis=1).to_numpy(),F[sf,'away'][mask]+F[sf,'home'][mask]),('FULL_TOTAL',d.loc[mask,['away_full_runs','home_full_runs']].sum(axis=1).to_numpy(),F[sf,'away'][mask]+F[sf,'home'][mask]+F[sg,'away_post'][mask]+F[sg,'home_post'][mask])]:ts.append({'slice_type':'MONTH','slice_value':month,'market':m,**metrics(y,mu)})
 pd.DataFrame(ts).to_csv(OUT/'temporal_stability.csv',index=False)
 d[['game_pk','date','start','away_team','home_team','away_starting_pitcher_id','home_starting_pitcher_id','away_starter_era','home_starter_era','away_expected_outs_governed','home_expected_outs_governed','away_team_runs_prior','home_team_runs_prior','away_bullpen_ra','home_bullpen_ra','park_run_factor_prior','away_f5_runs','home_f5_runs','away_full_runs','home_full_runs','split','source_path','source_sha256']].to_csv(OUT/'starter_led_population_manifest.csv',index=False)
 return means

def report(d,L,G,sf,sg,means):
 f=L.query("stage==@sf and phase=='LATER_HOLDOUT' and market=='F5_TOTAL'").iloc[0];g=G.query("stage==@sg and phase=='LATER_HOLDOUT' and market=='FULL_TOTAL'").iloc[0];era=L.query("stage=='B_ERA_WORKLOAD' and phase=='LATER_HOLDOUT' and market=='F5_TOTAL'").iloc[0];const=L.query("stage=='A_CONSTANT' and phase=='LATER_HOLDOUT' and market=='F5_TOTAL'").iloc[0];decision='STARTER_LED_SCORING_IMPROVES_PREDICTION_FOUNDATION' if sf!='A_CONSTANT' and f.crps<const.crps and np.std(means['F5_TOTAL'])>.2 else 'STARTER_LED_SCORING_VALID_BELOW_PRACTICAL_BAR' if sf!='A_CONSTANT' else 'STARTER_LED_SCORING_NO_USEFUL_SIGNAL';ready='READY' if decision.endswith('IMPROVES_PREDICTION_FOUNDATION') else 'BELOW_BAR' if sf!='A_CONSTANT' else 'NOT_READY'
 w=pd.read_csv(OUT/'starter_workload_metrics.csv').query("phase=='LATER_HOLDOUT'");align=pd.read_csv(OUT/'starter_era_alignment.csv').query("phase=='LATER_HOLDOUT' and slice=='ALL'").iloc[0]
 text=f"""# MLB Starter-Led Scoring Prediction Foundation v1

`{decision}`

- Population: {len(d)} exact games, {d.date.min()}–{d.date.max()}, common splits {d.split.value_counts().to_dict()}.
- F5 selected on validation: `{sf}`. Holdout MAE/bias/CRPS {f.mae:.4f}/{f.bias:.4f}/{f.crps:.4f}; prediction SD {f.prediction_sd:.4f}. ERA+workload holdout CRPS {era.crps:.4f} versus constant {const.crps:.4f}.
- Full-game selected after the F5 freeze: `{sg}`. Holdout MAE/bias/CRPS {g.mae:.4f}/{g.bias:.4f}/{g.crps:.4f}; prediction SD {g.prediction_sd:.4f}.
- Expected starter workload holdout MAE {w.mae.mean()/3:.3f} innings and bias {w.bias.mean()/3:.3f} innings. ERA-only out-of-time F5 R² {align.era_only_r_squared:.4f}; classification: {'one useful component' if sf!='A_CONSTANT' else 'too noisy to use directly in this construction'}.
- Offense, park/environment, and bullpen effects are reported stepwise and cannot hide earlier deterioration. Frozen totals V1 and prior decomposed results are context-only because their exact compatible holdout has 79 rather than 314 games.
- Readiness: `F5_PREDICTION={ready}`, `FULL_GAME_PREDICTION={'READY' if sg!='A_CONSTANT' and ready=='READY' else 'BELOW_BAR' if sg!='A_CONSTANT' else 'NOT_READY'}`, `TEAM_TOTAL_PREDICTION={'BELOW_BAR' if sg!='A_CONSTANT' else 'NOT_READY'}`.
- Exact next step supported: {'preserve the transparent formula as a research benchmark and obtain broader clean starter/bullpen histories before any prospective shadow' if decision!='STARTER_LED_SCORING_NO_USEFUL_SIGNAL' else 'retain the constant control and do not create a starter-led prospective shadow'}.
- No sportsbook features, EV/Edge, deployment, existing-model mutation, or pipeline change occurred.
""";(OUT/'concise_mlb_starter_led_scoring_prediction_foundation_v1.md').write_text(text)

def main():
 OUT.mkdir(parents=True,exist_ok=True);d=population();audit(d);F=formulas(d);L,G,sf,sg,val,hold=choose(d,F);means=outputs(d,F,L,G,sf,sg,val,hold);report(d,L,G,sf,sg,means);files=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sha(p)}  {p.name}\n' for p in files))
if __name__=='__main__':main()
