#!/usr/bin/env python3
"""Bounded research-only lineup-confirmed coherent MLB scoring model v2."""
from __future__ import annotations
import glob,hashlib,json
from collections import defaultdict,deque
from pathlib import Path
import numpy as np,pandas as pd
from scipy.stats import nbinom,poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ROOT=Path(__file__).resolve().parents[3];OUT=ROOT/'artifacts/analysis/model_development/mlb_lineup_confirmed_scoring_prediction_v2/2026-08-12';SEED=20260812;CAP=30
TARGETS=['away_f5_runs','home_f5_runs','away_post_f5_runs','home_post_f5_runs']
TEAM=['league_rpg_prior','away_team_runs_prior','home_team_runs_prior','away_team_ra_prior','home_team_ra_prior','away_rest','home_rest','home_field']
LINEUP=['away_lineup_obp','home_lineup_obp','away_lineup_slg','home_lineup_slg','away_lineup_recent_ops','home_lineup_recent_ops','away_top_ops','home_top_ops','away_middle_ops','home_middle_ops','away_bottom_ops','home_bottom_ops','away_platoon_advantage','home_platoon_advantage','away_lineup_sparse','home_lineup_sparse']
STARTER=['away_starter_ra9','home_starter_ra9','away_starter_k_rate','home_starter_k_rate','away_starter_bb_rate','home_starter_bb_rate','away_starter_h_rate','home_starter_h_rate','away_starter_expected_outs','home_starter_expected_outs','away_starter_recent_ra9','home_starter_recent_ra9','away_starter_starts','home_starter_starts','away_offense_vs_starter','home_offense_vs_starter']
BULLPEN=['away_bullpen_ra','home_bullpen_ra','away_bullpen_recent_ra','home_bullpen_recent_ra','away_bullpen_workload3','home_bullpen_workload3']
ENV=['park_run_factor_prior','park_history_depth','day_game','doubleheader','month_sin','month_cos']
FULL=TEAM+LINEUP+STARTER+BULLPEN+ENV
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def pmf(mu,a=0):
 x=np.arange(CAP+1)
 if a>1e-9:r=1/a;p=r/(r+max(mu,.01));q=nbinom.pmf(x,r,p)
 else:q=poisson.pmf(x,max(mu,.01))
 q[-1]+=max(0,1-q.sum());return q/q.sum()
def conv(ps):
 q=np.array([1.])
 for p in ps:q=np.convolve(q,p)
 q=q[:CAP+1];q[-1]+=max(0,1-q.sum());return q/q.sum()
def crps(p,y):
 x=np.arange(len(p));return np.sum((np.cumsum(p)-(x>=int(y)))**2)
def rate(s,n,default):return s/n if n else default

def parse():
 games=[]
 for f in glob.glob(str(ROOT/'backend/mlb/data/external/statsapi/raw/2026/*/feed_live.json')):
  try:j=json.loads(Path(f).read_text())
  except:continue
  gd=j.get('gameData',{});live=j.get('liveData',{});box=live.get('boxscore',{}).get('teams',{});ls=live.get('linescore',{})
  if gd.get('status',{}).get('abstractGameState')!='Final':continue
  innings=ls.get('innings') or [];first=[]
  for n in range(1,6):
   z=next((x for x in innings if x.get('num')==n),None)
   if not z or z.get('away',{}).get('runs') is None or z.get('home',{}).get('runs') is None:first=[];break
   first.append((int(z['away']['runs']),int(z['home']['runs'])))
  if len(first)!=5:continue
  orders={s:box.get(s,{}).get('battingOrder') or [] for s in ['away','home']}
  if any(len(x)!=9 for x in orders.values()):continue
  teams=gd.get('teams',{});players=gd.get('players',{});full={s:int(ls['teams'][s]['runs']) for s in ['away','home']}
  starters={}
  for s in ['away','home']:
   cand=[]
   for key,v in box[s].get('players',{}).items():
    st=v.get('stats',{}).get('pitching',{})
    if st.get('gamesStarted')==1:cand.append((int(key.replace('ID','')),st))
   if len(cand)!=1:break
   starters[s]=cand[0]
  if len(starters)!=2:continue
  batstats={}
  for s in ['away','home']:
   for slot,pid in enumerate(orders[s],1):
    v=box[s]['players'].get(f'ID{pid}',{});st=v.get('stats',{}).get('batting',{});person=players.get(f'ID{pid}',{})
    batstats[int(pid)]={'pa':int(st.get('plateAppearances',0)),'h':int(st.get('hits',0)),'bb':int(st.get('baseOnBalls',0))+int(st.get('hitByPitch',0)),'tb':int(st.get('totalBases',0)),'hr':int(st.get('homeRuns',0)),'hand':(person.get('batSide') or {}).get('code'),'slot':slot}
  lineup_json={s:json.dumps([{'slot':i+1,'player_id':int(pid),'name':players.get(f'ID{pid}',{}).get('fullName'),'bat_hand':batstats[int(pid)]['hand']} for i,pid in enumerate(orders[s])],separators=(',',':')) for s in ['away','home']}
  pitch_hands={s:(players.get(f'ID{starters[s][0]}',{}).get('pitchHand') or {}).get('code') for s in starters}
  games.append({'game_pk':int(j['gamePk']),'date':gd['datetime']['officialDate'],'start':gd['datetime']['dateTime'],'away_id':int(teams['away']['id']),'home_id':int(teams['home']['id']),'away_team':teams['away']['abbreviation'],'home_team':teams['home']['abbreviation'],'venue_id':int((gd.get('venue') or {}).get('id') or 0),'venue':(gd.get('venue') or {}).get('name'),'day_game':gd['datetime'].get('dayNight')=='day','doubleheader':gd.get('game',{}).get('doubleHeader')!='N','orders':orders,'lineup_json':lineup_json,'batstats':batstats,'starters':starters,'pitch_hands':pitch_hands,'starter_names':{s:players.get(f'ID{starters[s][0]}',{}).get('fullName') for s in starters},'away_f5_runs':sum(x[0] for x in first),'home_f5_runs':sum(x[1] for x in first),'away_post_f5_runs':full['away']-sum(x[0] for x in first),'home_post_f5_runs':full['home']-sum(x[1] for x in first),'away_full_runs':full['away'],'home_full_runs':full['home'],'source_path':str(Path(f).relative_to(ROOT)),'source_sha256':sha(f)})
 return sorted(games,key=lambda x:(x['date'],x['start'],x['game_pk']))

def features(games):
 bats=defaultdict(lambda:{'pa':0,'h':0,'bb':0,'tb':0,'hr':0,'games':deque(maxlen=10)});pitch=defaultdict(lambda:{'outs':0,'r':0,'h':0,'bb':0,'k':0,'starts':0,'recent':deque(maxlen=5)});team=defaultdict(lambda:{'g':0,'rf':0,'ra':0,'last':None,'bp_r':0,'bp_g':0,'bp_recent':deque(maxlen=7),'work':deque(maxlen=3)});park=defaultdict(lambda:{'r':0,'g':0});league={'r':0,'g':0};rows=[]
 for g in games:
  r={k:g[k] for k in ['game_pk','date','start','away_team','home_team','venue_id','venue','source_path','source_sha256']};r.update({t:g[t] for t in TARGETS});r['away_full_runs']=g['away_full_runs'];r['home_full_runs']=g['home_full_runs'];r['away_starting_lineup_json']=g['lineup_json']['away'];r['home_starting_lineup_json']=g['lineup_json']['home'];r['away_starting_pitcher_id']=g['starters']['away'][0];r['home_starting_pitcher_id']=g['starters']['home'][0];r['away_starting_pitcher_name']=g['starter_names']['away'];r['home_starting_pitcher_name']=g['starter_names']['home'];r['away_pitcher_hand']=g['pitch_hands']['away'];r['home_pitcher_hand']=g['pitch_hands']['home'];r['lineup_contract']='OFFICIAL_FINAL_BOX_SCORE_STARTING_NINE_AS_CONFIRMED_LINEUP_PROXY'
  r['league_rpg_prior']=rate(league['r'],league['g'],9);r['home_field']=1;r['day_game']=int(g['day_game']);r['doubleheader']=int(g['doubleheader']);m=int(g['date'][5:7]);r['month_sin']=np.sin(2*np.pi*m/12);r['month_cos']=np.cos(2*np.pi*m/12);r['park_run_factor_prior']=rate(rate(park[g['venue_id']]['r'],park[g['venue_id']]['g'],r['league_rpg_prior']),r['league_rpg_prior'],1);r['park_history_depth']=park[g['venue_id']]['g']
  for side,opp in [('away','home'),('home','away')]:
   tid=g[f'{side}_id'];ts=team[tid];oppid=g[f'{opp}_id'];r[f'{side}_team_runs_prior']=rate(ts['rf'],ts['g'],r['league_rpg_prior']/2);r[f'{side}_team_ra_prior']=rate(ts['ra'],ts['g'],r['league_rpg_prior']/2);r[f'{side}_rest']=3 if ts['last'] is None else min(3,max(0,(pd.Timestamp(g['date'])-pd.Timestamp(ts['last'])).days-1));r[f'{side}_bullpen_ra']=rate(ts['bp_r'],ts['bp_g'],r['league_rpg_prior']/2);r[f'{side}_bullpen_recent_ra']=np.mean(ts['bp_recent']) if ts['bp_recent'] else r[f'{side}_bullpen_ra'];r[f'{side}_bullpen_workload3']=sum(ts['work'])
   vals=[]
   for slot,pid in enumerate(g['orders'][side],1):
    b=bats[int(pid)];obp=rate(b['h']+b['bb'],b['pa'],.32);slg=rate(b['tb'],max(b['pa']-b['bb'],1),.4);recent=np.mean(b['games']) if b['games'] else obp+slg;vals.append((slot,obp,slg,recent,b['pa'],g['batstats'][int(pid)]['hand']))
   w=np.array([1.12,1.08,1.06,1.05,1.02,1,.96,.92,.88]);r[f'{side}_lineup_obp']=np.average([x[1] for x in vals],weights=w);r[f'{side}_lineup_slg']=np.average([x[2] for x in vals],weights=w);r[f'{side}_lineup_recent_ops']=np.average([x[3] for x in vals],weights=w);r[f'{side}_top_ops']=np.mean([x[1]+x[2] for x in vals[:3]]);r[f'{side}_middle_ops']=np.mean([x[1]+x[2] for x in vals[3:6]]);r[f'{side}_bottom_ops']=np.mean([x[1]+x[2] for x in vals[6:]]);ph=g['pitch_hands'][opp];r[f'{side}_platoon_advantage']=np.mean([x[5] is not None and ph is not None and x[5]!=ph for x in vals]);r[f'{side}_lineup_sparse']=np.mean([x[4]<50 for x in vals])
   pid,st=g['starters'][side];ps=pitch[pid];r[f'{side}_starter_ra9']=rate(27*ps['r'],ps['outs'],4.5);r[f'{side}_starter_k_rate']=rate(ps['k'],ps['outs']+ps['h']+ps['bb'],.22);r[f'{side}_starter_bb_rate']=rate(ps['bb'],ps['outs']+ps['h']+ps['bb'],.08);r[f'{side}_starter_h_rate']=rate(ps['h'],ps['outs']+ps['h']+ps['bb'],.24);r[f'{side}_starter_expected_outs']=rate(ps['outs'],ps['starts'],15);r[f'{side}_starter_recent_ra9']=np.mean(ps['recent']) if ps['recent'] else r[f'{side}_starter_ra9'];r[f'{side}_starter_starts']=ps['starts']
  r['away_offense_vs_starter']=r['away_lineup_recent_ops']*(r['home_starter_ra9']/4.5);r['home_offense_vs_starter']=r['home_lineup_recent_ops']*(r['away_starter_ra9']/4.5);rows.append(r)
  # Update strictly after row materialization.
  for side,opp in [('away','home'),('home','away')]:
   tid=g[f'{side}_id'];runs=g[f'{side}_full_runs'];allowed=g[f'{opp}_full_runs'];ts=team[tid];ts['g']+=1;ts['rf']+=runs;ts['ra']+=allowed;ts['last']=g['date'];post=g[f'{opp}_post_f5_runs'];ts['bp_r']+=post;ts['bp_g']+=1;ts['bp_recent'].append(post);ts['work'].append(post)
   pid,st=g['starters'][side];ps=pitch[pid];outs=int(st.get('outs',0));rr=int(st.get('runs',0));ps['outs']+=outs;ps['r']+=rr;ps['h']+=int(st.get('hits',0));ps['bb']+=int(st.get('baseOnBalls',0));ps['k']+=int(st.get('strikeOuts',0));ps['starts']+=1;ps['recent'].append(27*rr/max(outs,1))
   for pid in g['orders'][side]:
    x=g['batstats'][int(pid)];b=bats[int(pid)];b['pa']+=x['pa'];b['h']+=x['h'];b['bb']+=x['bb'];b['tb']+=x['tb'];b['hr']+=x['hr'];b['games'].append(rate(x['h']+x['bb'],x['pa'],0)+rate(x['tb'],max(x['pa']-x['bb'],1),0))
  total=g['away_full_runs']+g['home_full_runs'];league['r']+=total;league['g']+=1;park[g['venue_id']]['r']+=total;park[g['venue_id']]['g']+=1
 d=pd.DataFrame(rows);d['split']=np.select([d.date<='2026-05-31',d.date<='2026-06-30'],['DEVELOPMENT','VALIDATION'],default='LATER_HOLDOUT');return d

def fit(name,X,y):
 if name=='CONTROL_A_NEGATIVE_BINOMIAL':return ('nb',y.mean(),max(0,(y.var()-y.mean())/y.mean()**2))
 if name=='MODEL_B_REGULARIZED_PLAYER_COUNT':return make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),PoissonRegressor(alpha=.5,max_iter=1000)).fit(X,y)
 return make_pipeline(SimpleImputer(strategy='median'),HistGradientBoostingRegressor(loss='poisson',max_iter=120,max_leaf_nodes=15,min_samples_leaf=35,learning_rate=.05,l2_regularization=1,early_stopping=False,random_state=SEED)).fit(X,y)
def pred(m,X):return np.repeat(m[1],len(X)) if isinstance(m,tuple) else np.maximum(.02,m.predict(X))
def mp(m,mu):return pmf(mu,m[2] if isinstance(m,tuple) else 0)

def evaluate(d):
 dev=d.split.eq('DEVELOPMENT');val=d.split.eq('VALIDATION');hold=d.split.eq('LATER_HOLDOUT');families=['CONTROL_A_NEGATIVE_BINOMIAL','MODEL_B_REGULARIZED_PLAYER_COUNT','MODEL_C_SHALLOW_HGB'];rows=[];vp={};mods={}
 for fam in families:
  mods[fam]={};vp[fam]={}
  for t in TARGETS:
   m=fit(fam,d.loc[dev,FULL],d.loc[dev,t]);mods[fam][t]=m
   for phase,mask in [('VALIDATION',val),('LATER_HOLDOUT',hold)]:
    mu=pred(m,d.loc[mask,FULL]);vp[fam,t,phase]=mu;y=d.loc[mask,t].to_numpy();rows.append({'model':fam,'component':t,'phase':phase,'games':len(y),'mae':np.mean(abs(mu-y)),'rmse':np.mean((mu-y)**2)**.5,'bias':np.mean(mu-y),'crps':np.mean([crps(mp(m,a),b) for a,b in zip(mu,y)]),'predicted_mean':np.mean(mu),'observed_mean':np.mean(y),'mean_calibration_ratio':np.mean(mu)/np.mean(y) if np.mean(y) else np.nan,'predicted_variance':np.var(mu),'observed_variance':np.var(y)})
 comp=pd.DataFrame(rows);scores=comp[comp.phase.eq('VALIDATION')].groupby('model').crps.mean();selected=scores.idxmin()
 # Ensemble only if fixed 50/50 B+C improves validation mean component CRPS.
 ens=[]
 for t in TARGETS:
  y=d.loc[val,t].to_numpy();mu=(vp['MODEL_B_REGULARIZED_PLAYER_COUNT',t,'VALIDATION']+vp['MODEL_C_SHALLOW_HGB',t,'VALIDATION'])/2;ens.append(np.mean([crps(pmf(a),b) for a,b in zip(mu,y)]))
 if np.mean(ens)<scores.min():selected='MODEL_D_ENSEMBLE_B_C'
 comp['selected_on_validation']=comp.model.eq(selected);comp.to_csv(OUT/'scoring_component_metrics.csv',index=False)
 pd.DataFrame([{'model':x,'validation_mean_component_crps':scores.get(x,np.mean(ens) if x=='MODEL_D_ENSEMBLE_B_C' else np.nan),'selected':x==selected} for x in families+['MODEL_D_ENSEMBLE_B_C']]).to_csv(OUT/'scoring_model_comparison.csv',index=False)
 # Refit frozen selection on dev+val.
 tr=dev|val;models={};mus={}
 for t in TARGETS:
  if selected=='MODEL_D_ENSEMBLE_B_C':
   models[t]=(fit('MODEL_B_REGULARIZED_PLAYER_COUNT',d.loc[tr,FULL],d.loc[tr,t]),fit('MODEL_C_SHALLOW_HGB',d.loc[tr,FULL],d.loc[tr,t]));mus[t]=(pred(models[t][0],d.loc[hold,FULL])+pred(models[t][1],d.loc[hold,FULL]))/2
  else:models[t]=fit(selected,d.loc[tr,FULL],d.loc[tr,t]);mus[t]=pred(models[t],d.loc[hold,FULL])
 return selected,models,mus,dev,val,hold

def derived(d,selected,models,mus,hold):
 h=d.loc[hold].reset_index(drop=True);markets={'F5_TOTAL':['away_f5_runs','home_f5_runs'],'FULL_GAME_TOTAL':TARGETS,'AWAY_FULL_RUNS':['away_f5_runs','away_post_f5_runs'],'HOME_FULL_RUNS':['home_f5_runs','home_post_f5_runs'],'AWAY_F5_RUNS':['away_f5_runs'],'HOME_F5_RUNS':['home_f5_runs']};raw=[];dist={}
 for i,r in h.iterrows():
  ps={t:pmf(mus[t][i]) for t in TARGETS}
  for name,parts in markets.items():
   p=conv([ps[t] for t in parts]);y=sum(r[t] for t in parts);mu=sum(mus[t][i] for t in parts);raw.append({'game_pk':r.game_pk,'date':r.date,'market':name,'actual':y,'mean':mu,'error':mu-y,'abs_error':abs(mu-y),'crps':crps(p,y)});dist[(r.game_pk,name)]=p
 x=pd.DataFrame(raw);x.groupby('market').agg(games=('game_pk','size'),mae=('abs_error','mean'),bias=('error','mean'),crps=('crps','mean')).reset_index().to_csv(OUT/'derived_prediction_metrics.csv',index=False)
 return h,x,dist

def ladders(h,dist):
 lines={'FULL_GAME_TOTAL':[7.5,8,8.5,9,9.5,10],'F5_TOTAL':[3.5,4,4.5,5,5.5],'AWAY_FULL_RUNS':[2.5,3,3.5,4,4.5,5],'HOME_FULL_RUNS':[2.5,3,3.5,4,4.5,5],'AWAY_F5_RUNS':[2.5,3,3.5,4],'HOME_F5_RUNS':[2.5,3,3.5,4]};rows=[]
 for market,ls in lines.items():
  for line in ls:
   probs=[];ys=[];push=0
   for _,r in h.iterrows():
    p=dist[(r.game_pk,market)];k=np.arange(len(p));pr=p[k>line].sum();actual=sum(r[t] for t in ({'FULL_GAME_TOTAL':TARGETS,'F5_TOTAL':TARGETS[:2],'AWAY_FULL_RUNS':[TARGETS[0],TARGETS[2]],'HOME_FULL_RUNS':[TARGETS[1],TARGETS[3]],'AWAY_F5_RUNS':[TARGETS[0]],'HOME_F5_RUNS':[TARGETS[1]]}[market]));push+=actual==line
    if actual!=line:probs.append(np.clip(pr/(1-(p[k==line].sum())),1e-8,1-1e-8));ys.append(actual>line)
   probs=np.array(probs);ys=np.array(ys,float);rows.append({'market':market,'line':line,'resolved':len(ys),'pushes':push,'brier':np.mean((probs-ys)**2),'log_loss':np.mean(-ys*np.log(probs)-(1-ys)*np.log(1-probs)),'calibration_bias':probs.mean()-ys.mean(),'probability_std':probs.std()})
 pd.DataFrame(rows).to_csv(OUT/'ladder_probability_calibration.csv',index=False)

def ablation(d,dev,val,hold):
 layers=[('A_TEAM_STATE',TEAM),('B_PLUS_LINEUP',TEAM+LINEUP),('C_PLUS_STARTER',TEAM+LINEUP+STARTER),('D_PLUS_BULLPEN',TEAM+LINEUP+STARTER+BULLPEN),('E_PLUS_ENVIRONMENT',FULL)];rows=[]
 for name,fs in layers:
  for phase,train,test in [('VALIDATION',dev,val),('LATER_HOLDOUT',dev|val,hold)]:
   mus={t:pred(fit('MODEL_B_REGULARIZED_PLAYER_COUNT',d.loc[train,fs],d.loc[train,t]),d.loc[test,fs]) for t in TARGETS}
   for market,parts in [('F5_TOTAL',TARGETS[:2]),('FULL_GAME_TOTAL',TARGETS),('TEAM_RUNS',TARGETS)]:
    vals=[];cs=[]
    if market=='TEAM_RUNS': combos=[[TARGETS[0],TARGETS[2]],[TARGETS[1],TARGETS[3]]]
    else:combos=[parts]
    for combo in combos:
     y=d.loc[test,combo].sum(axis=1).to_numpy();mu=sum(mus[t] for t in combo);vals.extend(abs(mu-y));cs.extend(crps(pmf(a),b) for a,b in zip(mu,y))
    rows.append({'layer':name,'phase':phase,'market':market,'mae':np.mean(vals),'crps':np.mean(cs)})
 pd.DataFrame(rows).to_csv(OUT/'feature_layer_ablation.csv',index=False)

def reports(d,h,x,selected,dev,val,hold):
 # temporal and practical fixed slices
 temp=[]
 for month,g in x.groupby(x.date.str[:7]):
  for m,z in g.groupby('market'):temp.append({'month':month,'market':m,'games':len(z),'mae':z.abs_error.mean(),'bias':z.error.mean(),'crps':z.crps.mean()})
 pd.DataFrame(temp).to_csv(OUT/'temporal_stability.csv',index=False)
 ec=[];hh=h.copy();means=x[x.market.eq('FULL_GAME_TOTAL')]['mean'].reset_index(drop=True);hh['expected_band']='CONSTANT' if means.nunique()<3 else pd.qcut(means,3,labels=['LOW','MID','HIGH']);hh['starter_depth']=pd.cut(hh[['away_starter_starts','home_starter_starts']].min(axis=1),[-1,2,10,999],labels=['SPARSE','MEDIUM','DEEP']);stress=hh[['away_bullpen_workload3','home_bullpen_workload3']].mean(axis=1);hh['bullpen_stress']='CONSTANT' if stress.nunique()<3 else pd.qcut(stress,3,labels=['LOW','MID','HIGH']);pf=hh.park_run_factor_prior;hh['park_band']='CONSTANT' if pf.nunique()<3 else pd.qcut(pf,3,labels=['LOW','MID','HIGH'])
 for field in ['expected_band','starter_depth','bullpen_stress','park_band']:
  for v,g in hh.groupby(field,observed=True):
   ids=set(g.game_pk);z=x[x.game_pk.isin(ids)&x.market.eq('FULL_GAME_TOTAL')];ec.append({'slice':field,'value':v,'games':len(z),'mae':z.abs_error.mean(),'bias':z.error.mean(),'crps':z.crps.mean()})
 pd.DataFrame(ec).to_csv(OUT/'error_characterization.csv',index=False)
 dm=pd.read_csv(OUT/'derived_prediction_metrics.csv').set_index('market');control=pd.read_csv(OUT/'scoring_component_metrics.csv');advance=selected!='CONTROL_A_NEGATIVE_BINOMIAL';read=[]
 for fam,ms in [('F5_TOTAL',['F5_TOTAL']),('FULL_GAME_TOTAL',['FULL_GAME_TOTAL']),('F5_TEAM_TOTAL',['AWAY_F5_RUNS','HOME_F5_RUNS']),('FULL_GAME_TEAM_TOTAL',['AWAY_FULL_RUNS','HOME_FULL_RUNS'])]:
  read.append({'family':fam,'declaration':'PREDICTION_READY' if advance and dm.loc[ms].crps.mean()<2.5 else 'VALID_BELOW_PRACTICAL_BAR' if advance else 'NOT_READY','holdout_games':len(h),'mean_crps':dm.loc[ms].crps.mean()})
 pd.DataFrame(read).to_csv(OUT/'related_market_prediction_readiness.csv',index=False)
 ab=pd.read_csv(OUT/'feature_layer_ablation.csv');base=ab.query("phase=='LATER_HOLDOUT' and layer=='A_TEAM_STATE'");full=ab.query("phase=='LATER_HOLDOUT' and layer=='E_PLUS_ENVIRONMENT'");gain=base.mae.mean()-full.mae.mean();declaration='LINEUP_CONFIRMED_SCORING_MODEL_MATERIAL_PREDICTION_ADVANCE' if gain>.15 and advance else 'LINEUP_CONFIRMED_SCORING_MODEL_IMPROVES_PREDICTION' if gain>0 and advance else 'LINEUP_CONFIRMED_SCORING_MODEL_NO_IMPROVEMENT'
 text=f"""# MLB Lineup-Confirmed Scoring Prediction v2

`{declaration}`

- Population: {len(d)} exact 9×2 official starting-lineup games, {d.date.min()} through {d.date.max()}; splits {d.split.value_counts().to_dict()}.
- Selected on validation only: `{selected}`. Exact new information: weighted prior batter OBP/SLG/recent production by lineup segment and platoon, starter RA9/K/BB/hit/workload/recent/depth matchup state, team bullpen post-F5 prevention/workload, and prior park/schedule environment.
- Holdout F5 total MAE/CRPS: {dm.loc['F5_TOTAL','mae']:.4f}/{dm.loc['F5_TOTAL','crps']:.4f}; full total {dm.loc['FULL_GAME_TOTAL','mae']:.4f}/{dm.loc['FULL_GAME_TOTAL','crps']:.4f}; away/home full-run MAE {dm.loc['AWAY_FULL_RUNS','mae']:.4f}/{dm.loc['HOME_FULL_RUNS','mae']:.4f}.
- Fixed regularized layer ablation changes mean holdout MAE by {gain:.4f} runs from team-only to full information. Layer-specific validation/holdout effects are retained without feature fishing.
- Frozen totals V1 and prior decomposed benchmarks are retained as external context only: exact population/split contracts differ. The directly compatible prior decomposed holdout full-total MAE was 3.9891; this model's later-holdout full-total MAE is {dm.loc['FULL_GAME_TOTAL','mae']:.4f}.
- Probability ladders report Brier/log loss/calibration with whole-line pushes separated. Readiness is probability-quality only.
- Limitations: final box-score starting nine is used as the official-lineup reconstruction proxy; historical confirmation timestamps are not retained. Weather is unavailable at trustworthy historical breadth. Bullpen state is team-level rather than individual-reliever availability. Current-slate demonstration was skipped because this offline run did not wait for confirmed lineups.
- No sportsbook features, EV, Edge, refit search, deployment, wager logic, pipeline modification, or public change occurred.
""";(OUT/'concise_mlb_lineup_confirmed_scoring_prediction_v2.md').write_text(text)

def manifests(d):
 d[['game_pk','date','start','away_team','home_team','venue','away_starting_pitcher_id','away_starting_pitcher_name','away_pitcher_hand','home_starting_pitcher_id','home_starting_pitcher_name','home_pitcher_hand','away_starting_lineup_json','home_starting_lineup_json','away_f5_runs','home_f5_runs','away_full_runs','home_full_runs','lineup_contract','split','source_path','source_sha256']].to_csv(OUT/'historical_lineup_population_manifest.csv',index=False)
 pd.DataFrame([{'feature':f,'strict_prior':True,'concept':'lineup-weighted batter state; top/middle/bottom, recent/season, platoon and sparse depth'} for f in LINEUP]).to_csv(OUT/'lineup_player_feature_manifest.csv',index=False)
 pd.DataFrame([{'feature':f,'strict_prior':True,'concept':'starter performance/workload/handed matchup state'} for f in STARTER]).to_csv(OUT/'starter_matchup_feature_manifest.csv',index=False)
 pd.DataFrame([{'feature':f,'strict_prior':True,'concept':'team-level post-F5 prevention and recent workload proxy','limitation':'individual reliever availability unavailable'} for f in BULLPEN]).to_csv(OUT/'bullpen_feature_manifest.csv',index=False)
 (OUT/'temporal_split_contract.json').write_text(json.dumps({'development':{'through':'2026-05-31','games':int((d.split=='DEVELOPMENT').sum())},'validation':{'from':'2026-06-01','through':'2026-06-30','games':int((d.split=='VALIDATION').sum())},'later_holdout':{'from':'2026-07-01','through':'2026-07-27','games':int((d.split=='LATER_HOLDOUT').sum())},'selection':'validation component CRPS only; no holdout tuning'},indent=2)+'\n')

def main():
 OUT.mkdir(parents=True,exist_ok=True);d=features(parse());manifests(d);selected,models,mus,dev,val,hold=evaluate(d);h,x,dist=derived(d,selected,models,mus,hold);ladders(h,dist);ablation(d,dev,val,hold);reports(d,h,x,selected,dev,val,hold);files=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sha(p)}  {p.name}\n' for p in files))
if __name__=='__main__':main()
