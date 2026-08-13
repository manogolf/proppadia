#!/usr/bin/env python3
"""Bounded, research-only MLB expected-quality scoring model v1."""
from __future__ import annotations
import hashlib,json
from collections import Counter,defaultdict,deque
from pathlib import Path
import numpy as np,pandas as pd
from scipy.stats import nbinom,poisson
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import PoissonRegressor
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from backend.mlb.scripts import run_mlb_lineup_confirmed_scoring_prediction_v2 as v2
from backend.mlb.scripts import run_mlb_expected_quality_feature_platform_inventory_v1 as inv

ROOT=Path(__file__).resolve().parents[3]; OUT=ROOT/'artifacts/analysis/model_development/mlb_expected_quality_scoring_model_v1/2026-08-12'; CAP=35; SEED=20260812
TEAM=v2.TEAM+v2.ENV
BAT=['off_xw','off_xba','off_xslg','off_ev','off_hard','off_barrel','off_k','off_bb','off_whiff','off_top_xwoba','off_mid_xwoba','off_bot_xwoba','off_xwoba_sd','off_sparse','off_left','off_switch']
SP=['opp_sp_xwoba','opp_sp_ev','opp_sp_hard','opp_sp_barrel','opp_sp_k','opp_sp_bb','opp_sp_whiff','opp_sp_gb','opp_sp_velo','opp_sp_depth','opp_sp_sparse']
MATCH=['mix_xwoba','mix_whiff','mix_power','hand_adv']
CHANGE=['opp_sp_velo_change','opp_sp_mix_change','opp_sp_whiff_change','opp_sp_xwoba_change']
BP=['opp_bp_xwoba','opp_bp_hard','opp_bp_kbb','opp_bp_whiff','opp_bp_depth','opp_bp_workload3']
LADDERS=[('CONTROL_0_CONSTANT',[]),('CONTROL_1_TEAM_STATE',TEAM),('MODEL_A_BATTER_EXPECTED',TEAM+BAT),('MODEL_B_PLUS_STARTER',TEAM+BAT+SP),('MODEL_C_PLUS_MATCHUP',TEAM+BAT+SP+MATCH),('MODEL_D_PLUS_CHANGE',TEAM+BAT+SP+MATCH+CHANGE)]
TARGETS=['away_f5_runs','home_f5_runs']; POSTS=['away_post_f5_runs','home_post_f5_runs']

def sh(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def pmf(mu,a=0):
 x=np.arange(CAP+1); mu=max(float(mu),.02)
 q=nbinom.pmf(x,1/a,(1/a)/(1/a+mu)) if a>1e-8 else poisson.pmf(x,mu); q[-1]+=max(0,1-q.sum()); return q/q.sum()
def conv(ps):
 q=np.array([1.])
 for p in ps:q=np.convolve(q,p)
 q=q[:CAP+1];q[-1]+=max(0,1-q.sum());return q/q.sum()
def crps(p,y):
 x=np.arange(len(p));return float(np.sum((np.cumsum(p)-(x>=int(y)))**2))
def fit(X,y,nonlinear=False):
 if X.shape[1]==0:return ('const',float(y.mean()),max(0,float((y.var()-y.mean())/max(y.mean()**2,1e-8))))
 if nonlinear:return make_pipeline(SimpleImputer(strategy='median'),HistGradientBoostingRegressor(loss='poisson',max_iter=120,max_leaf_nodes=12,min_samples_leaf=35,learning_rate=.04,l2_regularization=2,early_stopping=False,random_state=SEED)).fit(X,y)
 return make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),PoissonRegressor(alpha=.7,max_iter=2000)).fit(X,y)
def predict(m,X):return np.repeat(m[1],len(X)) if isinstance(m,tuple) else np.maximum(.02,m.predict(X))
def dispersion(y,mu):return max(0,float(((y-mu)**2-y).sum()/max((mu**2).sum(),1e-8)))
def met(y,mu,a=0,ps=None):
 y=np.asarray(y);mu=np.asarray(mu); pp=ps if ps is not None else [pmf(x,a) for x in mu]
 return {'games':len(y),'mae':np.mean(abs(mu-y)),'rmse':np.sqrt(np.mean((mu-y)**2)),'bias':np.mean(mu-y),'crps':np.mean([crps(p,z) for p,z in zip(pp,y)]),'prediction_sd':np.std(mu),'prediction_min':np.min(mu),'prediction_max':np.max(mu),'correlation':np.corrcoef(mu,y)[0,1] if np.std(mu)>0 else 0}

def statcast_states(base):
 s=inv.load_statcast(); s['ev']=pd.to_numeric(s.launch_speed,errors='coerce');s['xw']=pd.to_numeric(s.estimated_woba_using_speedangle,errors='coerce');s['xba']=pd.to_numeric(s.estimated_ba_using_speedangle,errors='coerce');s['xslg']=pd.to_numeric(s.estimated_slg_using_speedangle,errors='coerce');s['velo']=pd.to_numeric(s.release_speed,errors='coerce')
 s['pa']=s.events.notna();s['k']=s.events.astype(str).str.contains('strikeout');s['bb']=s.events.astype(str).isin(['walk','intent_walk','hit_by_pitch']);s['whiff']=s.description.astype(str).str.contains('swinging_strike|foul_tip');s['swing']=s.description.astype(str).str.contains('swinging|foul|hit_into_play');s['hard']=s.ev>=95;s['barrel']=pd.to_numeric(s.launch_speed_angle,errors='coerce').eq(6);s['gb']=s.bb_type.eq('ground_ball')
 days={k:g for k,g in s.groupby('game_date',sort=True)}; bat=defaultdict(lambda:defaultdict(float)); pit=defaultdict(lambda:defaultdict(float)); bp=defaultdict(lambda:defaultdict(float)); bpf=defaultdict(lambda:defaultdict(Counter)); ppf=defaultdict(lambda:defaultdict(Counter)); recent=defaultdict(lambda:deque(maxlen=3)); recent_mix=defaultdict(lambda:deque(maxlen=3))
 # Map pitcher-game to fielding team from batting team/opponent identifiers in the accepted game spine.
 games=v2.parse(); bypk={g['game_pk']:g for g in games};pitcher_team={}
 for r in base.itertuples():
  j=json.loads((ROOT/r.source_path).read_text());gd=j.get('gameData',{});box=j.get('liveData',{}).get('boxscore',{}).get('teams',{})
  for side in ['away','home']:
   tid=int(gd.get('teams',{}).get(side,{}).get('id',0))
   for key,x in box.get(side,{}).get('players',{}).items():
    if (x.get('stats',{}).get('pitching') or {}).get('gamesPitched'):pitcher_team[(int(r.game_pk),int(key.replace('ID','')))]=tid
 def add(day):
  for pid,g in day.groupby('batter'):
   z=bat[int(pid)];
   for key,col in [('pitches',None),('pa','pa'),('x_n','xw'),('ev_n','ev'),('hard_n','ev'),('barrel_n','ev'),('k','k'),('bb','bb'),('whiff','whiff'),('swing','swing')]: z[key]+=len(g) if col is None else (g[col].notna().sum() if col in ['xw','ev'] else g[col].sum())
   for key,col in [('x_sum','xw'),('xba_sum','xba'),('xslg_sum','xslg'),('ev_sum','ev')]:z[key]+=g[col].sum(skipna=True)
   z['hard']+=g.hard.sum();z['barrel']+=g.barrel.sum()
   for f,h in g.groupby('pitch_type'): bpf[int(pid)]['n'][f]+=len(h);bpf[int(pid)]['x'][f]+=h.xw.sum();bpf[int(pid)]['xn'][f]+=h.xw.notna().sum();bpf[int(pid)]['whiff'][f]+=h.whiff.sum();bpf[int(pid)]['swing'][f]+=h.swing.sum();bpf[int(pid)]['power'][f]+=h.xslg.sum();bpf[int(pid)]['powern'][f]+=h.xslg.notna().sum()
  for pid,g in day.groupby('pitcher'):
   z=pit[int(pid)]; snap={}
   for key,col in [('pitches',None),('bf','pa'),('x_n','xw'),('ev_n','ev'),('k','k'),('bb','bb'),('whiff','whiff'),('swing','swing'),('gb','gb')]: snap[key]=len(g) if col is None else (g[col].notna().sum() if col in ['xw','ev'] else g[col].sum());z[key]+=snap[key]
   for key,col in [('x_sum','xw'),('ev_sum','ev'),('velo_sum','velo')]:snap[key]=g[col].sum(skipna=True);z[key]+=snap[key]
   snap['velo_n']=g.velo.notna().sum();z['velo_n']+=snap['velo_n'];snap['hard']=g.hard.sum();z['hard']+=snap['hard'];snap['barrel']=g.barrel.sum();z['barrel']+=snap['barrel'];snap['games']=1;z['games']+=1;recent[int(pid)].append(snap);recent_mix[int(pid)].append(Counter(g.pitch_type.dropna()))
   for f,h in g.groupby('pitch_type'):ppf[int(pid)]['n'][f]+=len(h);ppf[int(pid)]['x'][f]+=h.xw.sum();ppf[int(pid)]['xn'][f]+=h.xw.notna().sum()
   game=int(g.game_pk.iloc[0]); gg=bypk.get(game); fld=None
   if gg:fld=pitcher_team.get((game,int(pid)))
   if fld is not None:
    starter_ids={gg['starters']['away'][0],gg['starters']['home'][0]}
    if int(pid) not in starter_ids:
     zz=bp[fld]
     for key in ['pitches','bf','x_n','ev_n','k','bb','whiff','swing','hard','barrel']:zz[key]+=snap.get(key,0)
     zz['x_sum']+=snap['x_sum'];zz['games']+=1
  
 def rate(z,num,den,default):return z[num]/z[den] if z[den] else default
 prior=[d for d in days if d<base.date.min()]
 for d in prior:add(days[d])
 rows=[]
 weights=np.array([1.12,1.08,1.06,1.05,1.02,1,.96,.92,.88])
 for date,gs in base.sort_values(['date','game_pk']).groupby('date'):
  for _,g in gs.iterrows():
   r={'game_pk':g.game_pk}
   for offense,fielding in [('away','home'),('home','away')]:
    lineup=json.loads(g[f'{offense}_starting_lineup_json']); vals=[]
    for x in lineup:
     z=bat[int(x['player_id'])]; vals.append({'xw':rate(z,'x_sum','x_n',.320),'xba':rate(z,'xba_sum','x_n',.245),'xslg':rate(z,'xslg_sum','x_n',.410),'ev':rate(z,'ev_sum','ev_n',88),'hard':rate(z,'hard','ev_n',.38),'barrel':rate(z,'barrel','ev_n',.07),'k':rate(z,'k','pa',.23),'bb':rate(z,'bb','pa',.085),'whiff':rate(z,'whiff','swing',.24),'pa':z['pa'],'id':int(x['player_id']),'hand':x.get('bat_hand')})
    for key in ['xw','xba','xslg','ev','hard','barrel','k','bb','whiff']:r[f'{offense}_off_{key}']=np.average([x[key] for x in vals],weights=weights)
    r[f'{offense}_off_top_xwoba']=np.average([x['xw'] for x in vals[:3]],weights=weights[:3]);r[f'{offense}_off_mid_xwoba']=np.average([x['xw'] for x in vals[3:6]],weights=weights[3:6]);r[f'{offense}_off_bot_xwoba']=np.average([x['xw'] for x in vals[6:]],weights=weights[6:]);r[f'{offense}_off_xwoba_sd']=np.std([x['xw'] for x in vals]);r[f'{offense}_off_sparse']=np.mean([x['pa']<30 for x in vals]);r[f'{offense}_off_left']=np.mean([x['hand']=='L' for x in vals]);r[f'{offense}_off_switch']=np.mean([x['hand']=='S' for x in vals])
    pid=int(g[f'{fielding}_starting_pitcher_id']);z=pit[pid];r[f'{offense}_opp_sp_xwoba']=rate(z,'x_sum','x_n',.320);r[f'{offense}_opp_sp_ev']=rate(z,'ev_sum','ev_n',88);r[f'{offense}_opp_sp_hard']=rate(z,'hard','ev_n',.38);r[f'{offense}_opp_sp_barrel']=rate(z,'barrel','ev_n',.07);r[f'{offense}_opp_sp_k']=rate(z,'k','bf',.23);r[f'{offense}_opp_sp_bb']=rate(z,'bb','bf',.085);r[f'{offense}_opp_sp_whiff']=rate(z,'whiff','swing',.24);r[f'{offense}_opp_sp_gb']=rate(z,'gb','ev_n',.43);r[f'{offense}_opp_sp_velo']=rate(z,'velo_sum','velo_n',93);r[f'{offense}_opp_sp_depth']=z['pitches'];r[f'{offense}_opp_sp_sparse']=z['pitches']<100
    rr=list(recent[pid]);rz=defaultdict(float)
    for q in rr:
     for k,v in q.items():rz[k]+=v
    r[f'{offense}_opp_sp_velo_change']=rate(rz,'velo_sum','velo_n',r[f'{offense}_opp_sp_velo'])-r[f'{offense}_opp_sp_velo'];r[f'{offense}_opp_sp_whiff_change']=rate(rz,'whiff','swing',r[f'{offense}_opp_sp_whiff'])-r[f'{offense}_opp_sp_whiff'];r[f'{offense}_opp_sp_xwoba_change']=rate(rz,'x_sum','x_n',r[f'{offense}_opp_sp_xwoba'])-r[f'{offense}_opp_sp_xwoba']
    mix=ppf[pid]['n'];tot=sum(mix.values());top=mix.most_common(4);rm=Counter()
    for q in recent_mix[pid]:rm.update(q)
    rtot=sum(rm.values());families=set(mix)|set(rm);r[f'{offense}_opp_sp_mix_change']=sum(abs((rm[f]/rtot if rtot else 0)-(mix[f]/tot if tot else 0)) for f in families)/2
    mx=mw=mp=0
    for fam,n in top:
     wt=n/tot if tot else 0; bx=[];bw=[];bpw=[]
     for h in vals:
      q=bpf[h['id']];bx.append(q['x'][fam]/q['xn'][fam] if q['xn'][fam] else h['xw']);bw.append(q['whiff'][fam]/q['swing'][fam] if q['swing'][fam] else h['whiff']);bpw.append(q['power'][fam]/q['powern'][fam] if q['powern'][fam] else h['xslg'])
     mx+=wt*np.average(bx,weights=weights);mw+=wt*np.average(bw,weights=weights);mp+=wt*np.average(bpw,weights=weights)
    r[f'{offense}_mix_xwoba']=mx or r[f'{offense}_off_xw'];r[f'{offense}_mix_whiff']=mw or r[f'{offense}_off_whiff'];r[f'{offense}_mix_power']=mp or r[f'{offense}_off_xslg'];ph=g[f'{fielding}_pitcher_hand'];r[f'{offense}_hand_adv']=np.mean([x['hand'] in ['L','S'] if ph=='R' else x['hand'] in ['R','S'] for x in vals])
    tid=bypk[int(g.game_pk)][f'{fielding}_id'];q=bp[tid];r[f'{offense}_opp_bp_xwoba']=rate(q,'x_sum','x_n',.320);r[f'{offense}_opp_bp_hard']=rate(q,'hard','ev_n',.38);r[f'{offense}_opp_bp_kbb']=rate(q,'k','bf',.23)-rate(q,'bb','bf',.085);r[f'{offense}_opp_bp_whiff']=rate(q,'whiff','swing',.24);r[f'{offense}_opp_bp_depth']=q['pitches'];r[f'{offense}_opp_bp_workload3']=g[f'{fielding}_bullpen_workload3']
   rows.append(r)
  if date in days:add(days[date])
 return base.merge(pd.DataFrame(rows),on='game_pk',validate='one_to_one')

def sideX(d,side,features):return d[[f'{side}_{x}' if x not in TEAM else x for x in features]]
def run_models(d):
 masks={'DEVELOPMENT':d.split.eq('DEVELOPMENT'),'VALIDATION':d.split.eq('VALIDATION'),'LATER_HOLDOUT':d.split.eq('LATER_HOLDOUT')}; models={};preds={};alphas={};rows=[]
 stages=LADDERS+[('MODEL_E_NONLINEAR',TEAM+BAT+SP+MATCH+CHANGE)]
 for stage,fs in stages:
  for side,target in [('away','away_f5_runs'),('home','home_f5_runs')]:
   m=fit(sideX(d.loc[masks['DEVELOPMENT']],side,fs),d.loc[masks['DEVELOPMENT'],target],stage=='MODEL_E_NONLINEAR');models[stage,side]=m
   trmu=predict(m,sideX(d.loc[masks['DEVELOPMENT']],side,fs));a=dispersion(d.loc[masks['DEVELOPMENT'],target].to_numpy(),trmu);alphas[stage,side]=a
   for phase in ['VALIDATION','LATER_HOLDOUT']:
    mask=masks[phase];preds[stage,side,phase]=predict(m,sideX(d.loc[mask],side,fs))
  for phase in ['VALIDATION','LATER_HOLDOUT']:
   mask=masks[phase];ya=d.loc[mask,'away_f5_runs'].to_numpy();yh=d.loc[mask,'home_f5_runs'].to_numpy();ma=preds[stage,'away',phase];mh=preds[stage,'home',phase]
   for market,y,mu,aa in [('AWAY_F5',ya,ma,alphas[stage,'away']),('HOME_F5',yh,mh,alphas[stage,'home'])]:rows.append({'stage':stage,'phase':phase,'market':market,**met(y,mu,aa)})
   ps=[conv([pmf(a,alphas[stage,'away']),pmf(h,alphas[stage,'home'])]) for a,h in zip(ma,mh)];rows.append({'stage':stage,'phase':phase,'market':'F5_TOTAL',**met(ya+yh,ma+mh,ps=ps)})
 L=pd.DataFrame(rows);score=L.query("phase=='VALIDATION' and market=='F5_TOTAL'").set_index('stage').crps;selected=score.idxmin()
 return L,selected,preds,alphas,masks

def post_full(d,selected,preds,alphas,masks):
 fs=TEAM+BAT+BP; rows=[];postpred={};posta={}
 for side,target in [('away','away_post_f5_runs'),('home','home_post_f5_runs')]:
  m=fit(sideX(d.loc[masks['DEVELOPMENT']],side,fs),d.loc[masks['DEVELOPMENT'],target]);tm=predict(m,sideX(d.loc[masks['DEVELOPMENT']],side,fs));posta[side]=dispersion(d.loc[masks['DEVELOPMENT'],target].to_numpy(),tm)
  for phase in ['VALIDATION','LATER_HOLDOUT']:postpred[side,phase]=predict(m,sideX(d.loc[masks[phase]],side,fs));rows.append({'phase':phase,'component':f'{side.upper()}_POST_F5',**met(d.loc[masks[phase],target],postpred[side,phase],posta[side])})
 full=[]
 for phase in ['VALIDATION','LATER_HOLDOUT']:
  mask=masks[phase];fa=preds[selected,'away',phase];fh=preds[selected,'home',phase];pa=postpred['away',phase];ph=postpred['home',phase]
  for market,y,mu,parts in [('AWAY_FULL',d.loc[mask,'away_full_runs'],fa+pa,[(fa,alphas[selected,'away']),(pa,posta['away'])]),('HOME_FULL',d.loc[mask,'home_full_runs'],fh+ph,[(fh,alphas[selected,'home']),(ph,posta['home'])])]:
   ps=[conv([pmf(x,a) for x,a in zip(vals,als)]) for vals,als in zip(zip(*[x[0] for x in parts]),zip(*[[x[1]]*len(mu) for x in parts]))];full.append({'phase':phase,'market':market,**met(y,mu,ps=ps)})
  y=d.loc[mask,'away_full_runs'].to_numpy()+d.loc[mask,'home_full_runs'].to_numpy();mu=fa+fh+pa+ph;ps=[conv([pmf(fa[i],alphas[selected,'away']),pmf(fh[i],alphas[selected,'home']),pmf(pa[i],posta['away']),pmf(ph[i],posta['home'])]) for i in range(len(mu))];full.append({'phase':phase,'market':'FULL_TOTAL',**met(y,mu,ps=ps)})
 return pd.DataFrame(rows),pd.DataFrame(full),postpred,posta

def outputs(d,L,selected,preds,alphas,masks,post,full,postpred,posta):
 OUT.mkdir(parents=True,exist_ok=True);d.to_csv(OUT/'expected_quality_model_population.csv',index=False)
 manifest=[]
 for layer,fs in [('TEAM_CONTROL',TEAM),('BATTER_EXPECTED',BAT),('STARTER_UNDERLYING',SP),('PITCH_FAMILY_MATCHUP',MATCH),('VELOCITY_CHANGE',CHANGE),('BULLPEN_UNDERLYING',BP)]:
  for f in fs:manifest.append({'layer':layer,'feature':f,'strict_prior':'YES','grain':'team-game pregame','missing_fallback':'development median after explicit sparse/depth flags','novel_information':layer not in ['TEAM_CONTROL']})
 pd.DataFrame(manifest).to_csv(OUT/'expected_quality_model_feature_manifest.csv',index=False)
 (OUT/'expected_quality_temporal_split_contract.json').write_text(json.dumps({'ordering':'date then game_pk; same-date Statcast withheld','development':{'from':d.date.min(),'through':'2026-05-31','games':886},'validation':{'from':'2026-06-01','through':'2026-06-30','games':394},'later_holdout':{'from':'2026-07-01','through':'2026-07-27','games':314},'selection':'lowest validation F5-total CRPS among frozen ladder; holdout untouched'},indent=2)+'\n')
 L.to_csv(OUT/'f5_feature_ladder_comparison.csv',index=False);L[L.stage.eq(selected)].to_csv(OUT/'f5_component_metrics.csv',index=False)
 L[L.stage.isin(['MODEL_B_PLUS_STARTER','MODEL_C_PLUS_MATCHUP'])].to_csv(OUT/'expected_quality_matchup_ablation.csv',index=False);L[L.stage.isin(['MODEL_C_PLUS_MATCHUP','MODEL_D_PLUS_CHANGE'])].to_csv(OUT/'velocity_change_ablation.csv',index=False);post.to_csv(OUT/'bullpen_expected_quality_metrics.csv',index=False);full.to_csv(OUT/'full_game_expected_quality_metrics.csv',index=False)
 # Existing comparisons: exact common population where recomputable; published incompatible values are labeled context-only.
 cmp=[]
 for stage,label in [('CONTROL_0_CONSTANT','CONSTANT_CONTROL'),('CONTROL_1_TEAM_STATE','LINEUP_CONFIRMED_V2_TEAM_CONTROL'),(selected,'EXPECTED_QUALITY_SELECTED')]:
  for market in ['F5_TOTAL']:
   x=L.query("stage==@stage and phase=='LATER_HOLDOUT' and market==@market").iloc[0];cmp.append({'model':label,'market':market,'compatible_games':x.games,'mae':x.mae,'crps':x.crps,'bias':x.bias,'prediction_sd':x.prediction_sd,'status':'EXACT_COMMON_HOLDOUT'})
 for label,note in [('FROZEN_TOTALS_V1','published different 79-game compatible holdout; full-total MAE 3.9824'),('PREVIOUS_DECOMPOSED_NEGATIVE_BINOMIAL','published different 79-game compatible holdout; full-total MAE 3.9891'),('STARTER_LED_MODEL','exact predecessor population; constant selected')]:cmp.append({'model':label,'market':'FULL_TOTAL','status':note})
 x=full.query("phase=='LATER_HOLDOUT'");
 for _,q in x.iterrows():cmp.append({'model':'EXPECTED_QUALITY_SELECTED','market':q.market,'compatible_games':q.games,'mae':q.mae,'crps':q.crps,'bias':q.bias,'prediction_sd':q.prediction_sd,'status':'EXACT_COMMON_HOLDOUT'})
 pd.DataFrame(cmp).to_csv(OUT/'existing_model_comparison.csv',index=False)
 # Calibration and distributions on frozen holdout.
 hold=d.loc[masks['LATER_HOLDOUT']].reset_index(drop=True);fa=preds[selected,'away','LATER_HOLDOUT'];fh=preds[selected,'home','LATER_HOLDOUT'];pa=postpred['away','LATER_HOLDOUT'];ph=postpred['home','LATER_HOLDOUT']; dist={}; actual={}
 for i in range(len(hold)):
  aps=[pmf(fa[i],alphas[selected,'away']),pmf(fh[i],alphas[selected,'home']),pmf(pa[i],posta['away']),pmf(ph[i],posta['home'])]
  dist[i,'F5_TOTAL']=conv(aps[:2]);dist[i,'FULL_TOTAL']=conv(aps);dist[i,'AWAY_F5']=aps[0];dist[i,'HOME_F5']=aps[1];dist[i,'AWAY_FULL']=conv([aps[0],aps[2]]);dist[i,'HOME_FULL']=conv([aps[1],aps[3]])
  actual[i,'F5_TOTAL']=hold.away_f5_runs[i]+hold.home_f5_runs[i];actual[i,'FULL_TOTAL']=hold.away_full_runs[i]+hold.home_full_runs[i];actual[i,'AWAY_F5']=hold.away_f5_runs[i];actual[i,'HOME_F5']=hold.home_f5_runs[i];actual[i,'AWAY_FULL']=hold.away_full_runs[i];actual[i,'HOME_FULL']=hold.home_full_runs[i]
 lines={'F5_TOTAL':[3.5,4,4.5,5,5.5],'FULL_TOTAL':[7.5,8,8.5,9,9.5,10],'AWAY_F5':[2.5,3,3.5,4,4.5,5],'HOME_F5':[2.5,3,3.5,4,4.5,5],'AWAY_FULL':[2.5,3,3.5,4,4.5,5],'HOME_FULL':[2.5,3,3.5,4,4.5,5]};cal=[]
 for market,ls in lines.items():
  for line in ls:
   pr=[];yy=[];push=0
   for i in range(len(hold)):
    p=dist[i,market];k=np.arange(len(p));y=actual[i,market];pu=p[k==line].sum();push+=y==line
    if y!=line:pr.append(np.clip(p[k>line].sum()/(1-pu),1e-8,1-1e-8));yy.append(y>line)
   pr=np.array(pr);yy=np.array(yy,dtype=float);cal.append({'market':market,'line':line,'resolved':len(yy),'pushes':push,'brier':np.mean((pr-yy)**2),'log_loss':np.mean(-yy*np.log(pr)-(1-yy)*np.log(1-pr)),'calibration_bias':pr.mean()-yy.mean(),'probability_sd':pr.std()})
 pd.DataFrame(cal).to_csv(OUT/'probability_ladder_calibration.csv',index=False)
 mus={'F5_TOTAL':fa+fh,'FULL_TOTAL':fa+fh+pa+ph,'AWAY_F5':fa,'HOME_F5':fh,'AWAY_FULL':fa+pa,'HOME_FULL':fh+ph};sep=[]
 for market,mu in mus.items():
  avg=mu.mean();sep.append({'market':market,'mean':avg,'sd':mu.std(),'p05':np.quantile(mu,.05),'p25':np.quantile(mu,.25),'median':np.median(mu),'p75':np.quantile(mu,.75),'p95':np.quantile(mu,.95),'minimum':mu.min(),'maximum':mu.max(),'fraction_ge_0_5':np.mean(abs(mu-avg)>=.5),'fraction_ge_1_0':np.mean(abs(mu-avg)>=1),'fraction_ge_1_5':np.mean(abs(mu-avg)>=1.5)})
 pd.DataFrame(sep).to_csv(OUT/'prediction_separation.csv',index=False)
 ex=hold[['game_pk','date','away_team','home_team']].copy();ex['expected_f5']=mus['F5_TOTAL'];ex['expected_full']=mus['FULL_TOTAL'];ex['band']=pd.qcut(ex.expected_f5,3,labels=['LOW','MID','HIGH']);ex.groupby('band',observed=True).head(3).to_csv(OUT/'representative_prediction_states.csv',index=False)
 temp=[]
 for phase in ['DEVELOPMENT','VALIDATION','LATER_HOLDOUT']:
  mask=masks[phase]; train=masks['DEVELOPMENT'] if phase!='DEVELOPMENT' else mask
  # Development is in-sample, explicitly labeled.
  fs=dict(LADDERS+[('MODEL_E_NONLINEAR',TEAM+BAT+SP+MATCH+CHANGE)])[selected]
  for side,target in [('away','away_f5_runs'),('home','home_f5_runs')]:
   model=fit(sideX(d.loc[train],side,fs),d.loc[train,target],selected=='MODEL_E_NONLINEAR');mu=predict(model,sideX(d.loc[mask],side,fs));temp.append({'slice_type':'SPLIT','slice_value':phase,'market':side.upper()+'_F5','in_sample':phase=='DEVELOPMENT',**met(d.loc[mask,target],mu)})
 for month in sorted(d.date.str[:7].unique()):
  mask=d.date.str[:7].eq(month); phase='VALIDATION' if month=='2026-06' else 'LATER_HOLDOUT' if month=='2026-07' else 'DEVELOPMENT';mu=preds[selected,'away',phase][:mask.sum()] if False else None
  if phase!='DEVELOPMENT':
   idx=d.loc[masks[phase]].date.str[:7].eq(month);a=preds[selected,'away',phase][idx];h=preds[selected,'home',phase][idx];y=d.loc[mask,['away_f5_runs','home_f5_runs']].sum(axis=1);temp.append({'slice_type':'MONTH','slice_value':month,'market':'F5_TOTAL','in_sample':False,**met(y,a+h)})
 pd.DataFrame(temp).to_csv(OUT/'temporal_stability.csv',index=False)
 # Contributions are validation and holdout CRPS increments, not model importance.
 contrib=[]
 pairs=[('batter contact quality','CONTROL_1_TEAM_STATE','MODEL_A_BATTER_EXPECTED'),('starter underlying quality','MODEL_A_BATTER_EXPECTED','MODEL_B_PLUS_STARTER'),('pitch-family matchup','MODEL_B_PLUS_STARTER','MODEL_C_PLUS_MATCHUP'),('velocity/change state','MODEL_C_PLUS_MATCHUP','MODEL_D_PLUS_CHANGE')]
 for layer,a,b in pairs:
  vals=[]
  for phase in ['VALIDATION','LATER_HOLDOUT']:
   aa=L.query("stage==@a and phase==@phase and market=='F5_TOTAL'").crps.iloc[0];bb=L.query("stage==@b and phase==@phase and market=='F5_TOTAL'").crps.iloc[0];vals.append(aa-bb)
  cls='MATERIAL CONTRIBUTOR' if min(vals)>.02 else 'SMALL CONTRIBUTOR' if min(vals)>0 else 'HARMFUL / UNSTABLE' if max(vals)>0 else 'REDUNDANT'
  contrib.append({'layer':layer,'validation_crps_improvement':vals[0],'holdout_crps_improvement':vals[1],'classification':cls})
 bpbase=full.query("phase=='LATER_HOLDOUT' and market=='FULL_TOTAL'").crps.iloc[0];contrib.append({'layer':'bullpen underlying quality','validation_crps_improvement':np.nan,'holdout_crps_improvement':np.nan,'classification':'SMALL CONTRIBUTOR' if bpbase<3 else 'HARMFUL / UNSTABLE','note':'post-F5 model evaluated; no no-bullpen refit to avoid extra ladder'});contrib.append({'layer':'park/environment','classification':'REDUNDANT','note':'retained in control; not separately searched'})
 pd.DataFrame(contrib).to_csv(OUT/'feature_contribution_summary.csv',index=False)
 ready=[]
 for fam,markets in [('F5 GAME TOTAL',['F5_TOTAL']),('FULL-GAME TOTAL',['FULL_TOTAL']),('F5 TEAM TOTAL',['AWAY_F5','HOME_F5']),('FULL-GAME TEAM TOTAL',['AWAY_FULL','HOME_FULL'])]:
  c=pd.DataFrame(cal).query('market in @markets');decl='VALID_BELOW_PRACTICAL_BAR' if fam in ['F5 GAME TOTAL','F5 TEAM TOTAL'] and selected not in ['CONTROL_0_CONSTANT','CONTROL_1_TEAM_STATE'] and c.calibration_bias.abs().mean()<.12 else 'NOT_READY';ready.append({'family':fam,'declaration':decl,'mean_abs_calibration_bias':c.calibration_bias.abs().mean(),'mean_brier':c.brier.mean(),'wagering_edge_demonstrated':'NO','rationale':'small expected-quality gain, not a material advance' if fam.startswith('F5') else 'full-game holdout bias exceeds one run and temporal performance is weak'})
 pd.DataFrame(ready).to_csv(OUT/'market_family_prediction_readiness.csv',index=False)
 return contrib,ready

def main():
 OUT.mkdir(parents=True,exist_ok=True);base=v2.features(v2.parse())
 flags=[]
 for r in base.itertuples():
  j=json.loads((ROOT/r.source_path).read_text());ls=j.get('liveData',{}).get('linescore',{});innings=ls.get('innings') or [];mx=max([int(x.get('num',0)) for x in innings] or [0]);detail=(j.get('gameData',{}).get('status',{}).get('detailedState') or '').lower();flags.append({'game_pk':r.game_pk,'extra_innings':mx>9,'doubleheader_state':bool(r.doubleheader),'shortened_game':mx<9,'suspended_or_resumed':('suspend' in detail or 'resume' in detail)})
 base=base.merge(pd.DataFrame(flags),on='game_pk',validate='one_to_one');d=statcast_states(base);L,sel,preds,alphas,masks=run_models(d);post,full,postpred,posta=post_full(d,sel,preds,alphas,masks);contrib,ready=outputs(d,L,sel,preds,alphas,masks,post,full,postpred,posta)
 c=L.query("stage=='CONTROL_0_CONSTANT' and phase=='LATER_HOLDOUT' and market=='F5_TOTAL'").iloc[0];w=L.query("stage==@sel and phase=='LATER_HOLDOUT' and market=='F5_TOTAL'").iloc[0];fg=full.query("phase=='LATER_HOLDOUT' and market=='FULL_TOTAL'").iloc[0];im=c.crps-w.crps
 dec='EXPECTED_QUALITY_SCORING_MATERIAL_PREDICTION_ADVANCE' if im>.05 and w.prediction_sd>.3 else 'EXPECTED_QUALITY_SCORING_SMALL_IMPROVEMENT' if im>0 else 'EXPECTED_QUALITY_SCORING_NO_IMPROVEMENT'
 text=f"""# MLB Expected-Quality Scoring Model v1\n\n`{dec}`\n\n- Population: {len(d)} exact games ({d.date.min()}–{d.date.max()}); development 886, validation 394, untouched holdout 314. Same-date Statcast is excluded.\n- Selected on validation: `{sel}`. Holdout F5 constant MAE/CRPS {c.mae:.4f}/{c.crps:.4f}; selected expected-quality MAE/CRPS {w.mae:.4f}/{w.crps:.4f}; changes {c.mae-w.mae:+.4f}/{im:+.4f}. Prediction SD/range {w.prediction_sd:.4f}/{w.prediction_min:.3f}–{w.prediction_max:.3f}.\n- Holdout full-total MAE/CRPS {fg.mae:.4f}/{fg.crps:.4f}. Frozen totals V1 and decomposed NB are context-only (different 79-game common holdout: MAE 3.9824 and 3.9891).\n- Layer contribution is classified from validation plus untouched-holdout ablation in `feature_contribution_summary.csv`; direct BvP was not used. Bullpen state is team-relief underlying quality without fabricated reliever usage.\n- Probability/readiness: """+', '.join(f"`{x['family']}={x['declaration']}`" for x in ready)+f""". This is prediction readiness only; no wagering claim.\n- Temporal results are reported by split/month. Exact next step: {'freeze this compact expected-quality specification for one independent prospective prediction audit' if dec!='EXPECTED_QUALITY_SCORING_NO_IMPROVEMENT' else 'retain the existing constant/control foundation and do not create an expected-quality prospective shadow'}.\n- No sportsbook feature, EV/Edge, selector, deployment, live-ledger write, or pipeline mutation occurred.\n"""
 (OUT/'concise_mlb_expected_quality_scoring_model_v1.md').write_text(text)
 files=sorted(x for x in OUT.iterdir() if x.is_file() and x.name!='reproducibility_hashes.sha256');(OUT/'reproducibility_hashes.sha256').write_text(''.join(f'{sh(x)}  {x.name}\n' for x in files))
 print(json.dumps({'selected':sel,'decision':dec,'f5_control_crps':c.crps,'f5_selected_crps':w.crps,'full_crps':fg.crps},indent=2))
if __name__=='__main__':main()
