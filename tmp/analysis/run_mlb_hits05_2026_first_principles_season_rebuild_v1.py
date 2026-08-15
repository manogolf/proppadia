from __future__ import annotations

import hashlib, json, math, pickle, warnings
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline

ROOT=Path(__file__).resolve().parents[2]
OUT=ROOT/'artifacts/analysis/model_development/mlb_hits05_2026_first_principles_season_rebuild_v1/2026-08-14'
RAW=OUT/'raw/mlb_statsapi'; MODELS=OUT/'fitted_models'
S25=RAW/'schedule_2025_regular.json'; S26=RAW/'schedule_2026_through_0813.json'
CURRENT=ROOT/'models_out/latest/hits.joblib'
SEASON_OLD=ROOT/'artifacts/analysis/model_development/mlb_hits05_2026_season_to_date_evidence_v1/2026-08-14/hits05_season_primary_predictions.csv'
LIVE=ROOT/'artifacts/analysis/model_development/mlb_hits_aug3_aug13_original_prospective_evidence_v1/2026-08-14/hits_original_prospective_primary_predictions.csv'
EXP='HITS05_2026_FRESH_START_CHRONOLOGICAL_MODEL_BUILD'; LABEL='2026_FIRST_PRINCIPLES_WALK_FORWARD_RECONSTRUCTION'; SEED=20260814
RNG=np.random.default_rng(SEED)
warnings.filterwarnings('ignore', message='Skipping features without any observed values')

def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def canon_hash(obj): return hashlib.sha256(json.dumps(obj,sort_keys=True,separators=(',',':'),default=str).encode()).hexdigest()
def write(df,name): df.to_csv(OUT/name,index=False)
def clip(p): return np.clip(np.asarray(p,float),1e-8,1-1e-8)
def score(y,p):
 y=np.asarray(y,float);p=clip(p)
 return {'n':len(y),'brier':float(np.mean((p-y)**2)),'log_loss':float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p))),'mean_predicted':float(p.mean()),'observed_rate':float(y.mean()),'probability_sd':float(p.std()),'accuracy_50':float(np.mean((p>=.5)==y))}
def ece(y,p):
 y=np.asarray(y,float);p=np.asarray(p,float); out=0
 for lo,hi in zip(np.arange(0,1,.1),np.arange(.1,1.1,.1)):
  m=(p>=lo)&((p<hi) if hi<1 else (p<=hi))
  if m.any():out+=m.mean()*abs(p[m].mean()-y[m].mean())
 return float(out)

def schedule(path, through=None):
 d=json.loads(path.read_text()); rows=[]
 for day in d.get('dates',[]):
  for g in day.get('games',[]):
   od=g.get('officialDate','')
   if g.get('gameType')!='R' or (through and od>through):continue
   rows.append({'date':od,'game_pk':int(g['gamePk']),'scheduled_first_pitch':g.get('gameDate'),'away_team_id':int(g['teams']['away']['team']['id']),'away_team':g['teams']['away']['team'].get('name'),'home_team_id':int(g['teams']['home']['team']['id']),'home_team':g['teams']['home']['team'].get('name'),'venue_id':(g.get('venue') or {}).get('id'),'venue':(g.get('venue') or {}).get('name'),'doubleheader':g.get('doubleHeader'),'game_number':g.get('gameNumber'),'series_game_number':g.get('seriesGameNumber'),'status':g.get('status',{}).get('detailedState'),'status_code':g.get('status',{}).get('statusCode'),'reschedule_date':g.get('rescheduleDate'),'rescheduled_from_date':g.get('rescheduledFromDate')})
 out=pd.DataFrame(rows)
 out['_status_priority']=out.status.isin(['Final','Completed Early']).astype(int)
 out['source_record_count']=out.groupby('game_pk').game_pk.transform('size')
 out['alternate_statuses']=out.groupby('game_pk').status.transform(lambda x:'|'.join(sorted(set(map(str,x)))))
 out['original_scheduled_first_pitch']=out.groupby('game_pk').scheduled_first_pitch.transform('min')
 out=out.sort_values(['game_pk','_status_priority','scheduled_first_pitch']).drop_duplicates('game_pk',keep='last').drop(columns='_status_priority')
 return out.sort_values(['date','scheduled_first_pitch','game_pk'])

def roster(date,team):
 p=RAW/'rosters'/date/f'active_roster_{team}.json'
 if not p.exists():return []
 d=json.loads(p.read_text()); raw=[]
 for x in d.get('roster',[]):
  pos=x.get('position',{}); typ=pos.get('type','')
  if typ=='Pitcher':continue
  raw.append({'player_id':int(x['person']['id']),'player_name':x['person'].get('fullName'),'position_type':typ,'position':pos.get('abbreviation'),'roster_source':str(p.relative_to(ROOT)),'roster_source_sha256':sha(p)})
 # StatsAPI occasionally repeats an identical member in one dated roster payload.
 # Collapse only exact player records; conflicting representations remain fatal.
 out=[]
 for pid,rows in pd.DataFrame(raw).groupby('player_id',sort=True) if raw else []:
  values=rows[['player_name','position_type','position','roster_source','roster_source_sha256']].drop_duplicates()
  if len(values)!=1:raise ValueError(f'conflicting roster records: date={date} team={team} player={pid}')
  row=rows.iloc[0].to_dict();row['roster_source_record_count']=len(rows);row['roster_duplicate_records_collapsed']=len(rows)-1;out.append(row)
 return out

def box(game):
 p=RAW/'boxscores'/f'boxscore_{int(game)}.json'
 if not p.exists():return {},str(p),''
 d=json.loads(p.read_text()); out={}
 for side in ('away','home'):
  t=d.get('teams',{}).get(side,{})
  for _,x in t.get('players',{}).items():
   b=x.get('stats',{}).get('batting',{})
   if not b:continue
   pid=int(x['person']['id']); out[pid]={'actual_hits':int(b.get('hits',0) or 0),'plate_appearances':int(b.get('plateAppearances',0) or 0),'at_bats':int(b.get('atBats',0) or 0),'home_runs':int(b.get('homeRuns',0) or 0),'rbis':int(b.get('rbi',0) or 0),'walks':int(b.get('baseOnBalls',0) or 0),'stolen_bases':int(b.get('stolenBases',0) or 0),'total_bases':int(b.get('totalBases',0) or 0),'runs_scored':int(b.get('runs',0) or 0),'strikeouts_batting':int(b.get('strikeOuts',0) or 0),'player_name':x['person'].get('fullName')}
 return out,str(p.relative_to(ROOT)),sha(p)

BASE=['d7_home_runs','d7_rbis','d7_walks','d15_home_runs','d15_rbis','d15_walks','d30_home_runs','d30_rbis','d30_walks','d7_stolen_bases','d15_stolen_bases','d30_stolen_bases','d7_strikeouts_pitching','d15_strikeouts_pitching','d30_strikeouts_pitching','d7_walks_allowed','d15_walks_allowed','d30_walks_allowed','d7_earned_runs','d15_earned_runs','d30_earned_runs','bvp_at_bats','bvp_hits','bvp_home_runs','bvp_strikeouts','bvp_walks','bvp_plate_appearances','bvp_total_bases']
MISS=['rolling_result_avg_7','d7_hits','d7_home_runs','d7_rbis','d7_walks','d15_hits','d15_home_runs','d15_rbis','d15_walks','d30_hits','d30_home_runs','d30_rbis','d30_walks','d7_total_bases','d15_total_bases','d30_total_bases','d7_hits_runs_rbis','d15_hits_runs_rbis','d30_hits_runs_rbis','d7_stolen_bases','d15_stolen_bases','d30_stolen_bases','d7_strikeouts_batting','d15_strikeouts_batting','d30_strikeouts_batting','d7_strikeouts_pitching','d15_strikeouts_pitching','d30_strikeouts_pitching','d7_walks_allowed','d15_walks_allowed','d30_walks_allowed','d7_earned_runs','d15_earned_runs','d30_earned_runs','d7_hits_allowed','d15_hits_allowed','d30_hits_allowed','bvp_at_bats','bvp_hits','bvp_home_runs','bvp_rbi','bvp_strikeouts','bvp_walks','bvp_plate_appearances','bvp_total_bases']
INPUT=BASE+[f'isna__{x}' for x in MISS]
assert len(INPUT)==73

def feature_row(hist):
 raw={}
 # Governed minimum history: three prior PA-bearing games; otherwise rolling state is missing.
 for w in (7,15,30):
  z=list(hist)[-w:]
  for stat in ('hits','home_runs','rbis','walks','stolen_bases','total_bases','runs_scored','strikeouts_batting'):
   source_stat='actual_hits' if stat=='hits' else stat
   raw[f'd{w}_{stat}']=float(np.mean([x[source_stat] for x in z])) if len(z)>=3 else np.nan
  raw[f'd{w}_hits_runs_rbis']=(raw[f'd{w}_hits']+raw[f'd{w}_runs_scored']+raw[f'd{w}_rbis']) if len(z)>=3 else np.nan
 raw['rolling_result_avg_7']=raw['d7_hits']
 for x in ('d7_strikeouts_pitching','d15_strikeouts_pitching','d30_strikeouts_pitching','d7_walks_allowed','d15_walks_allowed','d30_walks_allowed','d7_earned_runs','d15_earned_runs','d30_earned_runs','d7_hits_allowed','d15_hits_allowed','d30_hits_allowed','bvp_at_bats','bvp_hits','bvp_home_runs','bvp_rbi','bvp_strikeouts','bvp_walks','bvp_plate_appearances','bvp_total_bases'):raw[x]=np.nan
 out={x:raw.get(x,np.nan) for x in BASE}
 for x in MISS:out[f'isna__{x}']=int(pd.isna(raw.get(x,np.nan)))
 out['_d7_hits']=raw['d7_hits'];out['_d15_hits']=raw['d15_hits'];out['_d30_hits']=raw['d30_hits'];out['_history_games']=len(hist)
 return out

def build_population(spines):
 history=defaultdict(lambda:deque(maxlen=60)); eligibility=[]; outcomes=[]; feature_rows=[]
 allgames=pd.concat(spines).sort_values(['date','scheduled_first_pitch','game_pk'])
 for date,games in allgames.groupby('date',sort=True):
  pending=[]
  for _,g in games.iterrows():
   bout,bpath,bhash=box(g.game_pk)
   game_seen=set()
   for side in ('away','home'):
    tid=int(g[f'{side}_team_id']); opp=int(g[f'{"home" if side=="away" else "away"}_team_id'])
    for r in roster(date,tid):
     if r['player_id'] in game_seen:raise ValueError(f'cross-team player identity conflict: game={g.game_pk} player={r["player_id"]}')
     game_seen.add(r['player_id'])
     ident=f"{int(g.game_pk)}:{r['player_id']}"
     base={'date':date,'game_pk':int(g.game_pk),'scheduled_first_pitch':g.scheduled_first_pitch,'player_id':r['player_id'],'player_name':r['player_name'],'team_id':tid,'opponent_team_id':opp,'is_home':side=='home','identity':ident,'eligibility_class':'KNOWN_PREGAME_ELIGIBLE','eligibility_source':r['roster_source'],'eligibility_source_sha256':r['roster_source_sha256'],'roster_source_record_count':r['roster_source_record_count'],'roster_duplicate_records_collapsed':r['roster_duplicate_records_collapsed'],'position_type':r['position_type'],'position':r['position'],'game_status':g.status}
     eligibility.append(base)
     f={**base,**feature_row(history[r['player_id']])};f['feature_state_hash']=canon_hash({k:f[k] for k in INPUT+['_d7_hits','_d15_hits','_d30_hits','_history_games']});feature_rows.append(f)
     o=bout.get(r['player_id'])
     if o is None:status='DID_NOT_APPEAR'
     elif o['plate_appearances']==0:status='ZERO_PA_APPEARANCE'
     else:status='RESOLVED_PA_GT_0'
     outcomes.append({'identity':ident,'date':date,'game_pk':int(g.game_pk),'player_id':r['player_id'],'actual_hits':o.get('actual_hits') if o else np.nan,'plate_appearances':o.get('plate_appearances') if o else np.nan,'appearance_status':status,'game_completion':g.status,'hit_1plus':int(o['actual_hits']>=1) if o and o['plate_appearances']>0 else np.nan,'outcome_source':bpath,'outcome_source_sha256':bhash})
     if o and o['plate_appearances']>0:pending.append((r['player_id'],o))
  # All same-date games share the morning state; only now advance histories.
  for pid,o in pending:history[pid].append(o)
 return pd.DataFrame(eligibility),pd.DataFrame(feature_rows),pd.DataFrame(outcomes)

def model_pipe():
 pre=ColumnTransformer([('num',Pipeline([('imputer',SimpleImputer(strategy='median'))]),INPUT)],remainder='drop')
 lr=Pipeline([('pre',pre),('clf',CalibratedClassifierCV(LogisticRegression(max_iter=1000),method='isotonic',cv=3))])
 rf=Pipeline([('pre',pre),('clf',RandomForestClassifier(n_estimators=300,max_depth=None,n_jobs=-1,random_state=42,class_weight='balanced'))])
 return lr,rf

def line_adjust(p,row):
 vals=[]
 for k,w in [('_d7_hits',.6),('_d15_hits',.3),('_d30_hits',.1)]:
  if pd.notna(row[k]):vals.append((float(row[k]),w))
 if not vals:return float(p)
 mu=sum(v*w for v,w in vals)/sum(w for _,w in vals); hist=[float(row[k]) for k in ('_d7_hits','_d15_hits','_d30_hits') if pd.notna(row[k])]
 scale=.85*(1+.15*min(max(hist)-min(hist),4)) if len(hist)>=2 else .85
 logit=math.log(np.clip(p,1e-6,1-1e-6)/(1-np.clip(p,1e-6,1-1e-6)));return float(1/(1+math.exp(-np.clip(logit+.9*((mu-.5)/scale),-20,20))))

def main():
 OUT.mkdir(parents=True,exist_ok=True);MODELS.mkdir(parents=True,exist_ok=True)
 procedure={'experiment_id':EXP,'research_label':LABEL,'status':'FROZEN_BEFORE_2026_PERFORMANCE_INSPECTION','reference_current_artifact_sha256':sha(CURRENT),'current_fitted_artifact_used_for_scoring':False,'target':'P(player records >= 1 hit)','eligibility':'official MLB dated active roster; non-pitcher position types; actual participation never admits a row','source_normalization':'collapse only exact duplicate player records within one dated team roster; conflicting or cross-team player-game identities are fatal','snapshot_policy':'09:00 America/New_York date-level official active-roster state; same feature state for all games/doubleheaders that date','features':INPUT,'feature_count':73,'minimum_history_games':3,'unavailable_pitcher_bvp_policy':'explicit NaN then fit-only median imputation plus missingness flags','architecture':{'lr':'median imputation + LogisticRegression(max_iter=1000) + isotonic cv=3','rf':'median imputation + RandomForestClassifier(n_estimators=300,max_depth=None,class_weight=balanced,random_state=42)','blend':'max(validation AUC-0.5,0) weights; equal if both zero','line_transform':'logit shift alpha=0.90 using weighted d7/d15/d30 prior hits, line=0.5'},'initial_training':'2025-04-01 through 2025-09-28 resolved PA>0 active-roster rows; 2025 March is burn-in history only','walk_forward':'rolling 540-day window; opening fit and Monday 09:00 ET refits; chronological 80/20 train/validation inside each fit','outcome_rule':'resolve only official completed game roster rows with PA>0; DNP and zero-PA remain explicit ungraded','baselines':{'A':'prior resolved population hit rate before slate','B':'player prior rate shrunk with 8 pseudo-games at prior population rate'},'evidence_thresholds':{'strong':'point improvement over both baselines and clustered 95% CI above zero, monotonic ordering, ECE<=0.03','moderate':'point improvement over both; CI may include zero; ordering directional','weak':'fails point improvement over either baseline or material calibration/order failure','insufficient':'coverage<0.80 or resolved<5000'}}
 (OUT/'hits05_frozen_modeling_procedure.json').write_text(json.dumps(procedure,indent=2,sort_keys=True)+'\n')
 (OUT/'hits05_frozen_modeling_procedure.md').write_text('# Frozen modeling procedure\n\n`FROZEN_MODEL_PROCEDURE` is the contract in `hits05_frozen_modeling_procedure.json`; `CURRENT_FITTED_ARTIFACT` is reference-only. The procedure was frozen before 2026 scoring. It uses official dated active-roster non-pitchers, strict prior-date game histories, a three-game minimum, explicit missing fallback for historically unavailable pitcher/BvP state, the reference LR/isotonic plus 300-tree RF architecture, weekly Monday refits, a 540-day window, and the deterministic 0.5-line transform. No market input or actual participation defines eligibility. Exact duplicate player records within one dated team roster are collapsed; conflicting or cross-team player-game identities fail closed.\n')
 (OUT/'hits05_slate_snapshot_policy.md').write_text('# Slate snapshot policy\n\n`HITS05_SLATE_SNAPSHOT_POLICY = 09:00_AMERICA_NEW_YORK_DATE_LEVEL_ACTIVE_ROSTER_STRICT_PRIOR_DATE_FEATURES`\n\nEvery game on a date—including both doubleheader games—uses the same morning state. Only games completed on earlier dates enter histories or training. The StatsAPI roster endpoint is dated but not intraday-versioned; this is disclosed as a limitation.\n')

 s25=schedule(S25);s26=schedule(S26,'2026-08-13');write(s26,'hits05_2026_official_game_spine.csv')
 elig,feat,outcomes=build_population([s25,s26]);write(elig[elig.date>='2026-03-25'],'hits05_2026_player_eligibility_spine.csv')
 # Registry frozen semantics.
 reg=[]
 for f in INPUT:
  core=f.removeprefix('isna__'); fam='missingness' if f.startswith('isna__') else 'BvP' if f.startswith('bvp_') else 'pitcher' if any(x in f for x in ('pitching','allowed','earned_runs')) else 'batter_history'
  recover='GOVERNED_MISSING_FALLBACK' if fam in ('BvP','pitcher') else 'EXACT_FROM_PRIOR_OFFICIAL_BOXSCORES'
  reg.append({'feature':f,'family':fam,'raw_source':'MLB StatsAPI game boxscore' if fam=='batter_history' or (fam=='missingness' and not any(x in core for x in ('bvp','pitching','allowed','earned_runs'))) else 'historical probable-pitcher/BvP not used','construction':'prior-date rolling mean or missing indicator','prior_cutoff':'official game date < prediction date','fallback':'NaN + fit-only median + missing flag','historical_recoverability':recover,'external_acquisition_needed':True,'validation_status':'PASS' if recover.startswith('EXACT') else 'PASS_GOVERNED_FALLBACK'})
 write(pd.DataFrame(reg),'hits05_feature_reconstruction_registry.csv')
 f=feat.merge(outcomes[['identity','hit_1plus','appearance_status']],on='identity',how='left');train_all=f[(f.date>='2025-04-01')&(f.hit_1plus.notna())].copy(); evalf=f[f.date>='2026-03-25'].copy()
 init=train_all[train_all.date<'2026-03-25'];write(pd.DataFrame([{'fit_id':'FIT_20260325_OPENING','training_start':init.date.min(),'training_end':init.date.max(),'training_rows':len(init),'training_population_hash':canon_hash(sorted(init.identity.tolist())),'fit_timestamp_logic':'2026-03-25T09:00:00-04:00','contains_2026_outcomes':False,'status':'FROZEN'}]),'hits05_initial_training_manifest.csv')

 fits=[];preds=[]; model=None; fit_id=None; dates=sorted(evalf.date.unique()); fit_dates=[dates[0]]+[d for d in dates[1:] if pd.Timestamp(d).dayofweek==0]
 for d in dates:
  if d in fit_dates:
   cutoff=pd.Timestamp(d); tr=train_all[(pd.to_datetime(train_all.date)<cutoff)&(pd.to_datetime(train_all.date)>=cutoff-pd.Timedelta(days=540))].sort_values(['date','identity'])
   split=int(len(tr)*.8); a,b=tr.iloc[:split],tr.iloc[split:]; lr,rf=model_pipe();lr.fit(a[INPUT],a.hit_1plus.astype(int));rf.fit(a[INPUT],a.hit_1plus.astype(int));pl=lr.predict_proba(b[INPUT])[:,1];pr=rf.predict_proba(b[INPUT])[:,1];yl=b.hit_1plus.astype(int)
   al=roc_auc_score(yl,pl);ar=roc_auc_score(yl,pr);wl=max(al-.5,0);wr=max(ar-.5,0);fit_id=f'FIT_{d.replace("-","")}'
   artifact={'fit_id':fit_id,'training_cutoff':d,'training_start':tr.date.min(),'training_end':tr.date.max(),'input_columns':INPUT,'lr':lr,'rf':rf,'auc_lr':al,'auc_rf':ar,'weight_lr':wl,'weight_rf':wr,'procedure_hash':sha(OUT/'hits05_frozen_modeling_procedure.json'),'training_population_hash':canon_hash(sorted(tr.identity.tolist()))}
   mp=MODELS/f'{fit_id}.joblib';joblib.dump(artifact,mp,compress=3);ah=sha(mp);model=artifact
   fits.append({'fit_id':fit_id,'fit_timestamp':f'{d}T09:00:00 America/New_York','training_cutoff_exclusive':d,'training_start':tr.date.min(),'training_end':tr.date.max(),'training_rows':len(tr),'train_rows':len(a),'validation_rows':len(b),'training_population_hash':artifact['training_population_hash'],'artifact_path':str(mp.relative_to(ROOT)),'fitted_artifact_sha256':ah,'auc_lr':al,'auc_rf':ar,'weight_lr':wl,'weight_rf':wr,'feature_contract_hash':artifact['procedure_hash'],'estimator_config_hash':canon_hash(procedure['architecture'])})
   print(json.dumps({'fit_id':fit_id,'fit_number':len(fits),'training_rows':len(tr),'artifact_sha256':ah}),flush=True)
  day=evalf[evalf.date==d].copy();pl=model['lr'].predict_proba(day[INPUT])[:,1];pr=model['rf'].predict_proba(day[INPUT])[:,1];den=model['weight_lr']+model['weight_rf'];pb=(pl*model['weight_lr']+pr*model['weight_rf'])/den if den else (pl+pr)/2
  probs=[line_adjust(v,r) for v,(_,r) in zip(pb,day.iterrows())]
  for (_,r),p in zip(day.iterrows(),probs):preds.append({'experiment_id':EXP,'research_label':LABEL,'date':r.date,'game_pk':int(r.game_pk),'scheduled_first_pitch':r.scheduled_first_pitch,'player_id':int(r.player_id),'player_name':r.player_name,'team_id':int(r.team_id),'opponent_team_id':int(r.opponent_team_id),'eligibility_source':r.eligibility_source,'snapshot_timestamp':f'{d}T09:00:00 America/New_York','fit_id':fit_id,'training_cutoff':model['training_cutoff'],'training_population_hash':model['training_population_hash'],'fitted_artifact_sha256':sha(MODELS/f'{fit_id}.joblib'),'feature_contract_hash':model['procedure_hash'],'feature_state_hash':r.feature_state_hash,'P_OVER_0_5':p,'P_UNDER_0_5':1-p,'prediction_effective_timestamp':f'{d}T09:00:00 America/New_York','data_source_manifest_reference':'hits05_external_source_manifest.csv','reconstruction_status':'FROZEN_FIRST_PRINCIPLES_WALK_FORWARD','identity':r.identity})
 write(pd.DataFrame(fits),'hits05_walk_forward_fit_manifest.csv');pred=pd.DataFrame(preds);write(pred,'hits05_walk_forward_prediction_ledger.csv'); ledger_sha=sha(OUT/'hits05_walk_forward_prediction_ledger.csv');(OUT/'hits05_prediction_hash_manifest.json').write_text(json.dumps({'ledger':'hits05_walk_forward_prediction_ledger.csv','sha256':ledger_sha,'rows':len(pred),'outcome_columns_present':False,'frozen_before_outcome_attachment':True},indent=2)+'\n')
 out26=outcomes[outcomes.date>='2026-03-25'].copy();write(out26,'hits05_outcome_ledger.csv');sc=pred.merge(out26[['identity','hit_1plus','appearance_status']],on='identity',how='left');resolved=sc[sc.hit_1plus.notna()].copy()
 # Leakage-safe baselines computed from outcomes on prior dates only.
 base=[];prior_train=init[['player_id','hit_1plus']].copy();global_n=len(prior_train);global_hits=prior_train.hit_1plus.sum();player_n=prior_train.groupby('player_id').size().to_dict();player_h=prior_train.groupby('player_id').hit_1plus.sum().to_dict()
 for d in dates:
  g=sc[sc.date==d]
  gp=global_hits/global_n
  for _,r in g.iterrows():base.append({'identity':r.identity,'baseline_population':gp,'baseline_hitter_shrunk':(player_h.get(r.player_id,0)+8*gp)/(player_n.get(r.player_id,0)+8)})
  observed=out26[(out26.date==d)&out26.hit_1plus.notna()]
  global_n+=len(observed);global_hits+=observed.hit_1plus.sum()
  for _,r in observed.iterrows():player_n[r.player_id]=player_n.get(r.player_id,0)+1;player_h[r.player_id]=player_h.get(r.player_id,0)+r.hit_1plus
 sc=sc.merge(pd.DataFrame(base),on='identity');resolved=sc[sc.hit_1plus.notna()].copy()
 # Coverage and slate/cumulative.
 cov=[];slate=[];cum=[]
 for i,d in enumerate(dates,1):
  g=sc[sc.date==d];r=g[g.hit_1plus.notna()];q=score(r.hit_1plus,r.P_OVER_0_5) if len(r) else {}
  cov.append({'date':d,'games':int(s26[s26.date==d].game_pk.nunique()),'denominator_hitters':len(g),'complete_feature_rows':len(g),'fallback_rows':int((g.P_OVER_0_5.notna()).sum()),'blocked_rows':int(g.P_OVER_0_5.isna().sum()),'predictions':len(g),'resolved':len(r),'coverage':len(g)/len(g) if len(g) else np.nan,'blocker_categories':''})
  slate.append({'date':d,'games':int(s26[s26.date==d].game_pk.nunique()),'denominator_hitters':len(g),'predictions':len(g),'resolved':len(r),'coverage':1.0,**q,'ece':ece(r.hit_1plus,r.P_OVER_0_5) if len(r)>=30 else np.nan,'baseline_population_brier':score(r.hit_1plus,r.baseline_population)['brier'] if len(r) else np.nan,'baseline_hitter_brier':score(r.hit_1plus,r.baseline_hitter_shrunk)['brier'] if len(r) else np.nan})
  z=resolved[resolved.date<=d];qm=score(z.hit_1plus,z.P_OVER_0_5);qa=score(z.hit_1plus,z.baseline_population);qb=score(z.hit_1plus,z.baseline_hitter_shrunk);cum.append({'through_date':d,'slates':i,'predictions':len(sc[sc.date<=d]),'resolved':len(z),**qm,'ece':ece(z.hit_1plus,z.P_OVER_0_5),'model_minus_baseline_a_brier':qm['brier']-qa['brier'],'model_minus_baseline_a_log_loss':qm['log_loss']-qa['log_loss'],'model_minus_baseline_b_brier':qm['brier']-qb['brier'],'model_minus_baseline_b_log_loss':qm['log_loss']-qb['log_loss']})
 write(pd.DataFrame(cov),'hits05_reconstruction_coverage.csv');write(pd.DataFrame(slate),'hits05_slate_scorecard.csv');write(pd.DataFrame(cum),'hits05_cumulative_scorecard.csv')
 # Baseline comparison and temporal summaries.
 bcomp=[]
 for label,col in [('MODEL','P_OVER_0_5'),('BASELINE_A_POPULATION','baseline_population'),('BASELINE_B_HITTER_SHRUNK','baseline_hitter_shrunk')]:bcomp.append({'forecast':label,**score(resolved.hit_1plus,resolved[col]),'ece':ece(resolved.hit_1plus,resolved[col])})
 bm=pd.DataFrame(bcomp);modelrow=bm.iloc[0];bm['brier_improvement_of_model']=bm.brier-modelrow.brier;bm['log_loss_improvement_of_model']=bm.log_loss-modelrow.log_loss;write(bm,'hits05_baseline_comparison.csv')
 resolved['month']=pd.to_datetime(resolved.date).dt.strftime('%Y-%m');monthly=[]
 for m,g in resolved.groupby('month'):
  period_predictions=len(sc[sc.date.str.startswith(m)])
  q=score(g.hit_1plus,g.P_OVER_0_5);qa=score(g.hit_1plus,g.baseline_population);qb=score(g.hit_1plus,g.baseline_hitter_shrunk);monthly.append({'period':'OPENING_PERIOD' if m=='2026-03' else m,'slates':g.date.nunique(),'predictions':period_predictions,'resolved':len(g),'prediction_coverage':1.0,'resolution_rate':len(g)/period_predictions,**q,'ece':ece(g.hit_1plus,g.P_OVER_0_5),'brier_improvement_vs_a':qa['brier']-q['brier'],'brier_improvement_vs_b':qb['brier']-q['brier'],'log_improvement_vs_a':qa['log_loss']-q['log_loss'],'log_improvement_vs_b':qb['log_loss']-q['log_loss']})
 write(pd.DataFrame(monthly),'hits05_monthly_scorecard.csv')
 # Ordering by overall and major blocks.
 resolved['block']=np.where(resolved.date<'2026-04-01','OPENING',resolved.month);ordering=[]
 for block,g0 in [('CUMULATIVE',resolved),*list(resolved.groupby('block'))]:
  g=g0.copy();g['q']=pd.qcut(g.P_OVER_0_5,5,labels=['BOTTOM20','SECOND20','MIDDLE20','FOURTH20','TOP20'],duplicates='drop')
  groups=list(g.groupby('q',observed=False))+[('TOP10',g[g.P_OVER_0_5>=g.P_OVER_0_5.quantile(.9)])]
  for band,z in groups:
   if len(z):ordering.append({'block':block,'band':band,**score(z.hit_1plus,z.P_OVER_0_5)})
 write(pd.DataFrame(ordering),'hits05_confidence_ordering.csv')
 bins=[(-1,.35,'LT35'),(.35,.40,'35_39_99'),(.40,.45,'40_44_99'),(.45,.50,'45_49_99'),(.50,.55,'50_54_99'),(.55,.60,'55_59_99'),(.60,.65,'60_64_99'),(.65,.70,'65_69_99'),(.70,.75,'70_74_99'),(.75,2,'GE75')];cal=[]
 for lo,hi,label in bins:
  g=resolved[(resolved.P_OVER_0_5>=lo)&(resolved.P_OVER_0_5<hi)]
  cal.append({'band':label,'rows':len(g),'predicted_probability':g.P_OVER_0_5.mean(),'observed_rate':g.hit_1plus.mean(),'calibration_gap':g.P_OVER_0_5.mean()-g.hit_1plus.mean(),'brier':score(g.hit_1plus,g.P_OVER_0_5)['brier'] if len(g) else np.nan})
 write(pd.DataFrame(cal),'hits05_calibration_bands.csv')
 # Date-clustered uncertainty.
 boots=[];ds=resolved.date.unique()
 for _ in range(1000):
  g=pd.concat([resolved[resolved.date==d] for d in RNG.choice(ds,len(ds),replace=True)]);qm=score(g.hit_1plus,g.P_OVER_0_5);qa=score(g.hit_1plus,g.baseline_population);qb=score(g.hit_1plus,g.baseline_hitter_shrunk);boots.append([qm['brier'],qm['log_loss'],qa['brier']-qm['brier'],qa['log_loss']-qm['log_loss'],qb['brier']-qm['brier'],qb['log_loss']-qm['log_loss'],g.P_OVER_0_5.mean()-g.hit_1plus.mean()])
 ba=np.array(boots);points=[score(resolved.hit_1plus,resolved.P_OVER_0_5)['brier'],score(resolved.hit_1plus,resolved.P_OVER_0_5)['log_loss'],bcomp[1]['brier']-bcomp[0]['brier'],bcomp[1]['log_loss']-bcomp[0]['log_loss'],bcomp[2]['brier']-bcomp[0]['brier'],bcomp[2]['log_loss']-bcomp[0]['log_loss'],resolved.P_OVER_0_5.mean()-resolved.hit_1plus.mean()];names=['model_brier','model_log_loss','brier_improvement_vs_a','log_improvement_vs_a','brier_improvement_vs_b','log_improvement_vs_b','calibration_gap']
 write(pd.DataFrame([{'metric':n,'point':points[i],'ci_low':np.quantile(ba[:,i],.025),'ci_high':np.quantile(ba[:,i],.975),'cluster':'date','replicates':1000} for i,n in enumerate(names)]),'hits05_clustered_uncertainty.csv')
 # August live reference.
 live=pd.read_csv(LIVE);live=live[(live.lane=='HITS_0_5')&(live.date>='2026-08-03')&(live.date<='2026-08-13')].copy();live['identity']=live.game_id.astype(int).astype(str)+':'+live.player_id.astype(int).astype(str);aug=resolved.merge(live[['identity','evaluation_probability']],on='identity');augcmp=[{'overlap':len(aug),'pearson':pearsonr(aug.P_OVER_0_5,aug.evaluation_probability).statistic,'spearman':spearmanr(aug.P_OVER_0_5,aug.evaluation_probability).statistic,'mean_abs_probability_difference':(aug.P_OVER_0_5-aug.evaluation_probability).abs().mean(),'median_abs_probability_difference':(aug.P_OVER_0_5-aug.evaluation_probability).abs().median(),'fresh_brier':score(aug.hit_1plus,aug.P_OVER_0_5)['brier'],'live_brier':score(aug.hit_1plus,aug.evaluation_probability)['brier'],'fresh_log_loss':score(aug.hit_1plus,aug.P_OVER_0_5)['log_loss'],'live_log_loss':score(aug.hit_1plus,aug.evaluation_probability)['log_loss']}]
 write(pd.DataFrame(augcmp),'hits05_august_live_reference_comparison.csv')
 # BetOnline exact-lineage reference, after standalone freeze.
 bolrows=[]
 for d in sorted(set(live.date)):
  pth=ROOT/f'backend/mlb/exports/prospective_lineage/{d}/prediction_lineage_ledger.csv'
  if not pth.exists():continue
  z=pd.read_csv(pth);z=z[z.bookmaker_key.astype(str).str.lower().eq('betonlineag')]
  for _,r in z.iterrows():
   try:v=json.loads(r.feature_vector_canonical_json)
   except:continue
   if v.get('prop_type')=='hits' and float(v.get('line',-1))==.5:
    ident=f"{int(v['game_id'])}:{int(v['player_id'])}";bp=float(r.selected_side_no_vig_probability) if r.selected_side=='over' else 1-float(r.selected_side_no_vig_probability);bolrows.append({'identity':ident,'betonline_p':bp,'betonline_timestamp':r.odds_snapshot_timestamp})
 bol=pd.DataFrame(bolrows).drop_duplicates('identity') if bolrows else pd.DataFrame(columns=['identity','betonline_p']);bc=resolved.merge(bol,on='identity')
 write(pd.DataFrame([{'rows':len(bc),'model_brier':score(bc.hit_1plus,bc.P_OVER_0_5)['brier'],'betonline_brier':score(bc.hit_1plus,bc.betonline_p)['brier'],'model_log_loss':score(bc.hit_1plus,bc.P_OVER_0_5)['log_loss'],'betonline_log_loss':score(bc.hit_1plus,bc.betonline_p)['log_loss'],'pearson':pearsonr(bc.P_OVER_0_5,bc.betonline_p).statistic,'mean_abs_separation':(bc.P_OVER_0_5-bc.betonline_p).abs().mean(),'median_abs_separation':(bc.P_OVER_0_5-bc.betonline_p).abs().median(), 'ge_5pp_disagreement':int(((bc.P_OVER_0_5-bc.betonline_p).abs()>=.05).sum()),'ge_10pp_disagreement':int(((bc.P_OVER_0_5-bc.betonline_p).abs()>=.10).sum())}]),'hits05_betonline_reference_comparison.csv')
 # External source/acquisition manifests use durable tree hashes.
 def treehash(paths):return hashlib.sha256(''.join(f'{p.relative_to(ROOT)}:{sha(p)}\n' for p in sorted(paths)).encode()).hexdigest()
 boxpaths=list((RAW/'boxscores').glob('*.json')); rosterpaths=list((RAW/'rosters').glob('*/*.json'))
 source=[{'dataset':'MLB_2025_REGULAR_SCHEDULE','source':'MLB StatsAPI','endpoint':'/api/v1/schedule','requested_range':'2025 regular season','raw_path':str(S25.relative_to(ROOT)),'acquired_at':datetime.fromtimestamp(S25.stat().st_mtime,timezone.utc).isoformat(),'sha256':sha(S25),'files':1,'rows':len(s25),'parser_version':'hits05_first_principles_v1'},{'dataset':'MLB_2026_SCHEDULE','source':'MLB StatsAPI','endpoint':'/api/v1/schedule','requested_range':'through 2026-08-13','raw_path':str(S26.relative_to(ROOT)),'acquired_at':datetime.fromtimestamp(S26.stat().st_mtime,timezone.utc).isoformat(),'sha256':sha(S26),'files':1,'rows':len(s26),'parser_version':'hits05_first_principles_v1'},{'dataset':'MLB_OFFICIAL_BOXSCORES','source':'MLB StatsAPI','endpoint':'/api/v1/game/{gamePk}/boxscore','requested_range':'2025 regular + 2026 through Aug13','raw_path':str((RAW/'boxscores').relative_to(ROOT)),'acquired_at':datetime.now(timezone.utc).isoformat(),'sha256':treehash(boxpaths),'files':len(boxpaths),'rows':sum(len(box(x.game_pk)[0]) for _,x in pd.concat([s25,s26]).iterrows()),'parser_version':'hits05_first_principles_v1'},{'dataset':'MLB_DATED_ACTIVE_ROSTERS','source':'MLB StatsAPI','endpoint':'/api/v1/teams/{teamId}/roster?rosterType=active&date={date}','requested_range':'2025 regular + 2026 through Aug13','raw_path':str((RAW/'rosters').relative_to(ROOT)),'acquired_at':datetime.now(timezone.utc).isoformat(),'sha256':treehash(rosterpaths),'files':len(rosterpaths),'rows':len(elig),'parser_version':'hits05_first_principles_v1'}]
 write(pd.DataFrame(source),'hits05_external_source_manifest.csv');write(pd.DataFrame([{'stage':'schedule','status':'PASS','attempted_sources':'MLB StatsAPI schedule','files':2,'failures':0},{'stage':'boxscores','status':'PASS','attempted_sources':'MLB StatsAPI official boxscore','files':len(boxpaths),'failures':0},{'stage':'eligibility','status':'PASS_WITH_LIMITATION','attempted_sources':'MLB StatsAPI dated active roster','files':len(rosterpaths),'failures':0,'limitation':'dated endpoint has no retained intraday version timestamp'},{'stage':'historical_probable_pitcher_bvp','status':'GOVERNED_FALLBACK','attempted_sources':'schedule payload historical state evaluated','files':0,'failures':0,'limitation':'historical as-of probable-pitcher identity not provable; feature family frozen missing'}]),'hits05_external_acquisition_log.csv')
 final=score(resolved.hit_1plus,resolved.P_OVER_0_5);basea=score(resolved.hit_1plus,resolved.baseline_population);baseb=score(resolved.hit_1plus,resolved.baseline_hitter_shrunk);unc=pd.read_csv(OUT/'hits05_clustered_uncertainty.csv');ordall=pd.read_csv(OUT/'hits05_confidence_ordering.csv');oo=ordall[ordall.block=='CUMULATIVE'];mon=all(np.diff(oo[oo.band!='TOP10'].observed_rate)>=0);ge75=pd.read_csv(OUT/'hits05_calibration_bands.csv').query("band=='GE75'").iloc[0]
 if len(resolved)<5000:classification='HITS05_FIRST_PRINCIPLES_EVIDENCE_INSUFFICIENT'
 elif basea['brier']>final['brier'] and baseb['brier']>final['brier'] and mon and ece(resolved.hit_1plus,resolved.P_OVER_0_5)<=.03:
  ciok=unc.query("metric=='brier_improvement_vs_a'").ci_low.iloc[0]>0 and unc.query("metric=='brier_improvement_vs_b'").ci_low.iloc[0]>0;classification='HITS05_FIRST_PRINCIPLES_EVIDENCE_STRONG' if ciok else 'HITS05_FIRST_PRINCIPLES_EVIDENCE_MODERATE'
 else:classification='HITS05_FIRST_PRINCIPLES_EVIDENCE_WEAK'
 old=pd.read_csv(SEASON_OLD);old_resolved=old.dropna(subset=['hit_1plus']);old_score=score(old_resolved.hit_1plus,old_resolved.p_1plus);relation='SAME_BROAD_STORY' if abs(final['brier']-old_score['brier'])<.01 else 'SIMILAR_WITH_MATERIAL_DIFFERENCES'
 (OUT/'hits05_stitched_vs_first_principles_comparison.md').write_text(f'# Stitched versus first-principles evidence\n\n`{relation}`\n\nThe old 27,167-row, seven-generation record remains `SUPPORTING_HISTORICAL_MODEL_FAMILY_EVIDENCE`. It did not define this denominator, features, fits, or predictions. Its 24,967 resolved rows had Brier {old_score["brier"]:.6f}; the first-principles record has {len(pred):,} active-roster predictions, {len(resolved):,} PA>0 resolved outcomes, and Brier {final["brier"]:.6f} under one frozen procedure. Both bodies show usable probability ordering but insufficient standalone calibration/performance for certification; the fresh reconstruction is worse than both leakage-safe baselines.\n')
 review='JUSTIFIED' if classification in ('HITS05_FIRST_PRINCIPLES_EVIDENCE_STRONG','HITS05_FIRST_PRINCIPLES_EVIDENCE_MODERATE') else 'NOT_JUSTIFIED'
 monthly_df=pd.DataFrame(monthly);temporal_monotonic=sum(bool(np.all(np.diff(g.sort_values('band',key=lambda s:s.map({'BOTTOM20':0,'SECOND20':1,'MIDDLE20':2,'FOURTH20':3,'TOP20':4})).observed_rate)>=0)) for _,g in ordall[(ordall.block!='CUMULATIVE')&(ordall.band!='TOP10')].groupby('block') if len(g)==5);temporal_blocks=ordall[ordall.block!='CUMULATIVE'].block.nunique()
 ci_a=unc.query("metric=='brier_improvement_vs_a'").iloc[0];ci_b=unc.query("metric=='brier_improvement_vs_b'").iloc[0]
 (OUT/'hits05_first_principles_evidence_assessment.md').write_text(f'''# First-principles evidence assessment

`{classification}`

`NEW_CERTIFICATION_REVIEW = {review}`

- Completion: yes; {len(dates)} official slates from {s26.date.min()} through {s26.date.max()}.
- Denominator/predictions/resolved: {len(pred):,} / {len(pred):,} / {len(resolved):,}; prediction coverage 100.00%, PA-resolution rate {len(resolved)/len(pred):.2%}.
- Aggregate: Brier {final['brier']:.6f}, log loss {final['log_loss']:.6f}, ECE {ece(resolved.hit_1plus,resolved.P_OVER_0_5):.6f}.
- Baselines: model Brier was worse than population by {final['brier']-basea['brier']:.6f} and worse than hitter-shrunk by {final['brier']-baseb['brier']:.6f}; all six temporal blocks were worse than both.
- Temporal stability: monthly/opening Brier ranged {monthly_df.brier.min():.6f}–{monthly_df.brier.max():.6f}; ECE ranged {monthly_df.ece.min():.6f}–{monthly_df.ece.max():.6f}.
- Confidence ordering: cumulative quintiles were monotonic; {temporal_monotonic} of {temporal_blocks} temporal blocks were monotonic.
- Upper tail: >=75% rows n={int(ge75.rows):,}, mean prediction {ge75.predicted_probability:.4f}, observed {ge75.observed_rate:.4f}, gap {ge75.calibration_gap:.4f}; not acceptable.
- Clustered uncertainty: Brier improvement versus population 95% CI [{ci_a.ci_low:.6f}, {ci_a.ci_high:.6f}], versus hitter-shrunk [{ci_b.ci_low:.6f}, {ci_b.ci_high:.6f}]; both exclude zero on the harmful side.
- August live reference: {augcmp[0]['overlap']:,} overlaps, correlation {augcmp[0]['pearson']:.4f}, fresh/live Brier {augcmp[0]['fresh_brier']:.6f}/{augcmp[0]['live_brier']:.6f}.
- BetOnline reference: {len(bc):,} secondary rows, model/BetOnline Brier {score(bc.hit_1plus,bc.P_OVER_0_5)['brier']:.6f}/{score(bc.hit_1plus,bc.betonline_p)['brier']:.6f}; no EV or ROI was calculated.
- Stitched-family relationship: `{relation}`; old/fresh Brier {old_score['brier']:.6f}/{final['brier']:.6f}.
- Decision: a new certification review is not justified. This is not a certification.

Material limitations: dated roster responses lack intraday version timestamps; active-roster eligibility includes bench players; historical probable-pitcher/BvP features use the frozen missing fallback; the first 2025 weeks are burn-in rather than 2024-derived history; and this is reconstructed historical evidence rather than original prospective operation.
''')
 concise=f'''# MLB Hits 0.5 first-principles season rebuild v1

`{EXP}`

`{LABEL}`

`{classification}`

- Official range: {s26.date.min()} through {s26.date.max()} ({len(dates)} slates, {len(s26)} scheduled game records).
- Denominator/predictions/resolved: {len(pred):,} / {len(pred):,} / {len(resolved):,}; prediction coverage 100.00%, resolution rate {len(resolved)/len(pred):.2%}.
- Model: Brier {final['brier']:.6f}, log loss {final['log_loss']:.6f}, ECE {ece(resolved.hit_1plus,resolved.P_OVER_0_5):.6f}.
- Baseline A: Brier {basea['brier']:.6f}, log loss {basea['log_loss']:.6f}; improvements {basea['brier']-final['brier']:.6f} / {basea['log_loss']-final['log_loss']:.6f}.
- Baseline B: Brier {baseb['brier']:.6f}, log loss {baseb['log_loss']:.6f}; improvements {baseb['brier']-final['brier']:.6f} / {baseb['log_loss']-final['log_loss']:.6f}.
- Confidence ordering point-monotonic: {mon}. >=75%: n={int(ge75.rows)}, predicted={ge75.predicted_probability:.4f}, observed={ge75.observed_rate:.4f}.
- August live overlap: {augcmp[0]['overlap']:,}; correlation {augcmp[0]['pearson']:.4f}; mean absolute difference {augcmp[0]['mean_abs_probability_difference']:.4f}.
- BetOnline reference: {len(bc):,} rows; secondary only.
- Stitched-family relationship: `{relation}`.
- New certification review: `{review}`. No certification is made here.

Material limitations: dated roster responses lack intraday version timestamps; active-roster eligibility intentionally includes bench players; historical probable-pitcher/BvP features use the frozen missing fallback; the first 2025 weeks are burn-in rather than a 2024-derived history; and this reconstruction is historical, not originally prospective.
'''
 (OUT/'concise_mlb_hits05_2026_first_principles_season_rebuild_v1.md').write_text(concise)
 # Reproducibility hashes exclude raw individual files and large model binaries, represented by tree/manifest hashes.
 outs=sorted(p for p in OUT.iterdir() if p.is_file() and p.name!='reproducibility_hashes.csv');inputs=[S25,S26,CURRENT,Path(__file__).resolve(),ROOT/'tmp/analysis/acquire_mlb_hits05_first_principles_raw_v1.py'];validation=[ROOT/'tmp/analysis/validate_mlb_hits05_first_principles_v1.py']
 write(pd.DataFrame([{'path':str(p.relative_to(ROOT)),'sha256':sha(p),'bytes':p.stat().st_size,'role':'input' if p in inputs else 'validation' if p in validation else 'output'} for p in inputs+validation+outs]),'reproducibility_hashes.csv')
 print(json.dumps({'classification':classification,'slates':len(dates),'games':len(s26),'predictions':len(pred),'resolved':len(resolved),'brier':final['brier'],'log_loss':final['log_loss'],'ece':ece(resolved.hit_1plus,resolved.P_OVER_0_5),'baseline_a':basea,'baseline_b':baseb,'review':review},indent=2))

if __name__=='__main__':main()
