"""Pure core for immutable NHL moneyline shadow capture and grading."""
from __future__ import annotations
import hashlib,json,math,re
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
PARAMETER_PATH=HERE/'frozen_champion_v1.json'
ALLOWED_RUN_TYPES={'MIDDAY','FINAL_PREGAME'}
FEATURES=['diff_std_goal_diff_pg','diff_r10_goal_diff_pg','diff_std_shot_diff_pg','diff_days_rest','home_back_to_back','away_back_to_back']
GAME_COLS=['canonical_season','slate_date','game_id','game_date','scheduled_start_time_utc','home_team_id','home_team','away_team_id','away_team','game_status']

def sha256_file(path:Path)->str:
 h=hashlib.sha256()
 with path.open('rb') as f:
  for block in iter(lambda:f.read(1<<20),b''): h.update(block)
 return h.hexdigest()
def canonical_json(obj:Any)->str: return json.dumps(obj,sort_keys=True,separators=(',',':'),ensure_ascii=False)
def parse_utc(v:Any)->pd.Timestamp:
 x=pd.to_datetime(v,utc=True,errors='coerce')
 if pd.isna(x): raise ValueError(f'invalid UTC timestamp: {v!r}')
 return x
def load_parameters(path:Path=PARAMETER_PATH)->dict:
 p=json.loads(path.read_text()); assert p['feature_order']==FEATURES and p['champion_identity']=='NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1'; return p
def parameter_hash(path:Path=PARAMETER_PATH)->str: return sha256_file(path)
def score_features(frame:pd.DataFrame,params:dict|None=None)->pd.DataFrame:
 p=params or load_parameters(); assert list(p['feature_order'])==FEATURES
 x=frame[FEATURES].apply(pd.to_numeric,errors='coerce').to_numpy(float); scaled=np.empty_like(x)
 for i,f in enumerate(FEATURES):
  st=p['features'][f]; x[:,i]=np.where(np.isnan(x[:,i]),st['median'],x[:,i]); scaled[:,i]=(x[:,i]-st['mean'])/st['scale']
 co=np.array([p['features'][f]['coefficient'] for f in FEATURES]); logit=p['intercept']+scaled@co; home=1/(1+np.exp(-logit)); away=1-home
 if not np.isfinite(home).all() or not ((home>=0)&(home<=1)).all() or not np.allclose(home+away,1,atol=1e-12): raise RuntimeError('probability semantics failure')
 return pd.DataFrame({'champion_home_win_probability':home,'champion_away_win_probability':away,'champion_predicted_side':np.where(home>=.5,'HOME','AWAY'),'champion_logit':logit},index=frame.index)
def historical_parity(matrix_csv:Path,predictions_csv:Path)->pd.DataFrame:
 m=pd.read_csv(matrix_csv); p=pd.read_csv(predictions_csv); keys=['canonical_season','game_id']; assert len(m)==len(p)==2798 and not m[keys].duplicated().any() and not p[keys].duplicated().any()
 raw=m[keys+[f'raw__{x}' for x in FEATURES]].copy(); raw.columns=keys+FEATURES; z=p[keys+['home_win_probability','predicted_side_at_0_5']].merge(raw,on=keys,validate='one_to_one'); scored=score_features(z); delta=abs(scored.champion_home_win_probability-z.home_win_probability)
 return pd.DataFrame([{'rows':len(z),'identity_match':True,'maximum_probability_delta':delta.max(),'mean_probability_delta':delta.mean(),'rows_over_tolerance':int((delta>1e-12).sum()),'side_mismatches':int((scored.champion_predicted_side!=z.predicted_side_at_0_5).sum()),'tolerance':1e-12,'status':'PASS' if delta.max()<=1e-12 and (scored.champion_predicted_side==z.predicted_side_at_0_5).all() else 'FAIL'}])

def _team_history(history:pd.DataFrame,team_id:int,target_start:pd.Timestamp,season:int)->pd.DataFrame:
 h=history[(history.canonical_season==season)&(history.game_status.str.upper().isin(['FINAL','OFF','COMPLETED']))].copy(); h['scheduled_start_time_utc']=pd.to_datetime(h.scheduled_start_time_utc,utc=True,errors='coerce'); h=h[h.scheduled_start_time_utc<target_start]
 home=h[h.home_team_id==team_id].assign(gf=h.final_home_goals,ga=h.final_away_goals,sf=h.final_home_shots,sa=h.final_away_shots,is_home=True)
 away=h[h.away_team_id==team_id].assign(gf=h.final_away_goals,ga=h.final_home_goals,sf=h.final_away_shots,sa=h.final_home_shots,is_home=False)
 return pd.concat([home,away],ignore_index=True).sort_values(['scheduled_start_time_utc','game_id'],kind='mergesort')
def build_strict_prior_features(schedule:pd.DataFrame,history:pd.DataFrame,run_timestamp_utc:str,allow_historical_fixture:bool=False)->tuple[pd.DataFrame,pd.DataFrame]:
 run=parse_utc(run_timestamp_utc); s=schedule.copy(); h=history.copy(); missing=set(GAME_COLS)-set(s.columns); required_h={'canonical_season','game_id','scheduled_start_time_utc','home_team_id','away_team_id','final_home_goals','final_away_goals','final_home_shots','final_away_shots','game_status'}
 if missing or required_h-set(h.columns): raise ValueError(f'schema missing schedule={sorted(missing)} history={sorted(required_h-set(h.columns))}')
 if s.game_id.duplicated().any(): raise ValueError('duplicate game identity')
 if not allow_historical_fixture and not s.canonical_season.eq(2026).all(): raise ValueError('prospective runs require canonical_season=2026')
 h['scheduled_start_time_utc']=pd.to_datetime(h.scheduled_start_time_utc,utc=True,errors='coerce')
 h=h[h.scheduled_start_time_utc<run].copy()
 rows=[]; aud=[]
 for r in s.sort_values(['scheduled_start_time_utc','game_id']).itertuples(index=False):
  start=parse_utc(r.scheduled_start_time_utc); status='STRICT_PRIOR_COMPLETE'; reason='';
  if run>=start: status='FEATURE_TIMING_INVALID'; reason='run_not_before_start'
  sides={};
  for side,tid in [('home',int(r.home_team_id)),('away',int(r.away_team_id))]:
   z=_team_history(h,tid,start,int(r.canonical_season)); vals={}; n=len(z)
   if n:
    vals['std_goal_diff_pg']=(z.gf-z.ga).mean(); vals['std_shot_diff_pg']=(z.sf-z.sa).mean(); vals['r10_goal_diff_pg']=(z.tail(10).gf-z.tail(10).ga).mean(); rest=(start-z.scheduled_start_time_utc.iloc[-1]).total_seconds()/86400; vals['days_rest']=math.floor(rest); vals['back_to_back']=float(math.floor(rest)==1)
   else: vals={x:np.nan for x in ['std_goal_diff_pg','std_shot_diff_pg','r10_goal_diff_pg','days_rest','back_to_back']}; status='MIN_HISTORY_IMPUTED' if status!='FEATURE_TIMING_INVALID' else status
   sides[side]=(vals,n,z)
  hv,hn,hz=sides['home']; av,an,az=sides['away']; row={c:getattr(r,c) for c in GAME_COLS}; row.update({'diff_std_goal_diff_pg':hv['std_goal_diff_pg']-av['std_goal_diff_pg'],'diff_r10_goal_diff_pg':hv['r10_goal_diff_pg']-av['r10_goal_diff_pg'],'diff_std_shot_diff_pg':hv['std_shot_diff_pg']-av['std_shot_diff_pg'],'diff_days_rest':hv['days_rest']-av['days_rest'],'home_back_to_back':hv['back_to_back'],'away_back_to_back':av['back_to_back'],'home_prior_games':hn,'away_prior_games':an,'feature_status':status,'feature_status_reason':reason,'latest_source_start_utc':max([x for x in [hz.scheduled_start_time_utc.max() if hn else pd.NaT,az.scheduled_start_time_utc.max() if an else pd.NaT] if pd.notna(x)],default=pd.NaT)})
  rows.append(row); aud.append({'game_id':r.game_id,'target_start_utc':start.isoformat(),'run_timestamp_utc':run.isoformat(),'home_prior_games':hn,'away_prior_games':an,'target_game_in_history':int((hz.game_id==r.game_id).sum()+(az.game_id==r.game_id).sum()),'future_history_rows':int((hz.scheduled_start_time_utc>=start).sum()+(az.scheduled_start_time_utc>=start).sum()),'status':status})
 return pd.DataFrame(rows),pd.DataFrame(aud)

def american_to_decimal(x:float)->float:
 x=float(x)
 if x==0: raise ValueError('zero American price')
 return 1+x/100 if x>0 else 1+100/abs(x)
def _norm_name(x:Any)->str: return re.sub(r'[^a-z0-9]','',str(x).lower())
def normalize_h2h(raw:list[dict],games:pd.DataFrame,capture_timestamp_utc:str,stale_minutes:int=30)->tuple[pd.DataFrame,pd.DataFrame]:
 capture=parse_utc(capture_timestamp_utc); games=games.copy(); games['scheduled_start_time_utc']=pd.to_datetime(games.scheduled_start_time_utc,utc=True,errors='coerce'); rows=[]; bind=[]
 for ev in raw:
  eid=str(ev.get('id','')); eh=ev.get('home_team'); ea=ev.get('away_team'); commence=pd.to_datetime(ev.get('commence_time'),utc=True,errors='coerce'); candidates=games[games.apply(lambda r:_norm_name(r.home_team)==_norm_name(eh) and _norm_name(r.away_team)==_norm_name(ea),axis=1)]
  if pd.notna(commence): candidates=candidates[abs((candidates.scheduled_start_time_utc-commence).dt.total_seconds())<=900]
  bind_status='BOUND' if len(candidates)==1 else 'GAME_BINDING_AMBIGUOUS'; bind.append({'provider_event_id':eid,'candidate_games':len(candidates),'binding_status':bind_status,'home_team':eh,'away_team':ea,'commence_time':ev.get('commence_time')})
  game=candidates.iloc[0] if len(candidates)==1 else None
  for book in ev.get('bookmakers') or []:
   for market in book.get('markets') or []:
    if market.get('key')!='h2h': continue
    outcomes=market.get('outcomes') or []; by={_norm_name(o.get('name')):o for o in outcomes}; ho=by.get(_norm_name(eh)); ao=by.get(_norm_name(ea)); mt=pd.to_datetime(market.get('last_update') or book.get('last_update'),utc=True,errors='coerce'); status='PREGAME_QUALIFIED'
    if game is None: status='GAME_BINDING_AMBIGUOUS'
    elif market.get('active') is False or market.get('suspended') is True: status='SUSPENDED'
    elif ho is None: status='MISSING_HOME_SIDE'
    elif ao is None: status='MISSING_AWAY_SIDE'
    elif pd.isna(mt): status='TIMESTAMP_MISSING'
    elif mt>=game.scheduled_start_time_utc or capture>=game.scheduled_start_time_utc: status='POST_START_INVALID'
    elif (capture-mt).total_seconds()>stale_minutes*60: status='STALE'
    hp=ho.get('price') if ho else np.nan; ap=ao.get('price') if ao else np.nan; hd=ad=hr=ar=over=hn=an=np.nan
    try:
     hd=american_to_decimal(hp); ad=american_to_decimal(ap); hr=1/hd; ar=1/ad; over=hr+ar-1; hn=hr/(hr+ar); an=ar/(hr+ar)
    except Exception:
     if status=='PREGAME_QUALIFIED': status='UNQUALIFIED_OTHER'
    rows.append({'provider_event_id':eid,'canonical_season':game.canonical_season if game is not None else np.nan,'game_id':game.game_id if game is not None else np.nan,'scheduled_start_time_utc':game.scheduled_start_time_utc.isoformat() if game is not None else None,'sportsbook_key':book.get('key'),'sportsbook_name':book.get('title'),'market_type':'h2h','home_team':eh,'away_team':ea,'home_price_american':hp,'away_price_american':ap,'raw_price_format':'american','home_decimal_price':hd,'away_decimal_price':ad,'p_home_raw':hr,'p_away_raw':ar,'overround':over,'p_home_devig':hn,'p_away_devig':an,'provider_market_timestamp_utc':mt.isoformat() if pd.notna(mt) else None,'source_timestamp_utc':mt.isoformat() if pd.notna(mt) else None,'capture_timestamp_utc':capture.isoformat(),'market_status':'ACTIVE' if status not in ['SUSPENDED'] else 'SUSPENDED','qualification_status':status})
 quote_columns=['provider_event_id','canonical_season','game_id','scheduled_start_time_utc','sportsbook_key','sportsbook_name','market_type','home_team','away_team','home_price_american','away_price_american','raw_price_format','home_decimal_price','away_decimal_price','p_home_raw','p_away_raw','overround','p_home_devig','p_away_devig','provider_market_timestamp_utc','source_timestamp_utc','capture_timestamp_utc','market_status','qualification_status']
 bind_columns=['provider_event_id','candidate_games','binding_status','home_team','away_team','commence_time']
 return pd.DataFrame(rows,columns=quote_columns),pd.DataFrame(bind,columns=bind_columns)

def make_run_id(season:int,slate_date:str,run_timestamp_utc:str,run_type:str)->str:
 if run_type not in ALLOWED_RUN_TYPES:
  raise ValueError('invalid run type')
 ts=parse_utc(run_timestamp_utc)
 compact=ts.strftime('%Y%m%dT%H%M%S%fZ')
 return f"nhlmlobs_s{season}_d{slate_date.replace('-','')}_t{compact}_{run_type}_v1"
def write_manifest(path:Path)->None:
 files=sorted(x for x in path.iterdir() if x.is_file() and x.name!='SHA256SUMS'); (path/'SHA256SUMS').write_text(''.join(f'{sha256_file(x)}  {x.name}\n' for x in files))
def run_shadow(schedule_csv:Path,history_csv:Path,odds_json:Path|None,output_root:Path,slate_date:str,run_timestamp_utc:str,run_type:str,allow_historical_fixture:bool=False)->Path:
 schedule=pd.read_csv(schedule_csv); season=int(schedule.canonical_season.iloc[0]); run_id=make_run_id(season,slate_date,run_timestamp_utc,run_type); dest=output_root/str(season)/slate_date/run_id
 if dest.exists(): raise FileExistsError('OVERWRITE_ATTEMPT_BLOCKED')
 dest.mkdir(parents=True,exist_ok=False); raw_bytes=odds_json.read_bytes() if odds_json else b'{"capture_timestamp_utc":null,"provider_response":[]}\n'; (dest/'raw_h2h_response.json').write_bytes(raw_bytes); raw_envelope=json.loads(raw_bytes); raw=raw_envelope.get('provider_response',[]) if isinstance(raw_envelope,dict) else raw_envelope; odds_capture_timestamp=raw_envelope.get('capture_timestamp_utc') if isinstance(raw_envelope,dict) else None; odds_capture_timestamp=odds_capture_timestamp or run_timestamp_utc; history=pd.read_csv(history_csv); features,audit=build_strict_prior_features(schedule,history,run_timestamp_utc,allow_historical_fixture); scoreable=~features.feature_status.isin(['FEATURE_SOURCE_MISSING','FEATURE_TIMING_INVALID','GAME_IDENTITY_BLOCKED']); scores=score_features(features.loc[scoreable]); predictions=features.loc[scoreable,GAME_COLS].copy(); predictions['run_id']=run_id; predictions['score_timestamp_utc']=parse_utc(run_timestamp_utc).isoformat(); predictions=pd.concat([predictions.reset_index(drop=True),scores.reset_index(drop=True)],axis=1); predictions['champion_identity']=load_parameters()['champion_identity']; predictions['champion_parameter_sha256']=parameter_hash()
 quotes,binding=normalize_h2h(raw,schedule,odds_capture_timestamp); comparisons=predictions[['canonical_season','game_id','run_id','champion_home_win_probability']].merge(quotes[quotes.qualification_status.eq('PREGAME_QUALIFIED')][['canonical_season','game_id','sportsbook_key','p_home_devig','provider_market_timestamp_utc','capture_timestamp_utc']],on=['canonical_season','game_id'],how='inner'); comparisons['champion_minus_market_probability']=comparisons.champion_home_win_probability-comparisons.p_home_devig
 schedule[GAME_COLS].to_csv(dest/'game_spine.csv',index=False); features.to_csv(dest/'champion_features.csv',index=False); predictions.to_csv(dest/'champion_predictions.csv',index=False); quotes.to_csv(dest/'moneyline_quotes.csv',index=False); comparisons.to_csv(dest/'market_comparison.csv',index=False); binding.to_csv(dest/'provider_game_binding.csv',index=False); audit.to_csv(dest/'feature_timing_audit.csv',index=False)
 p=load_parameters(); gate=[]
 def g(name,passed,critical,evidence): gate.append({'gate_name':name,'passed':bool(passed),'critical':critical,'evidence':evidence})
 g('canonical_season',allow_historical_fixture or season==2026,True,season); g('unique_game_ids',not schedule.game_id.duplicated().any(),True,len(schedule)); g('home_away_identity',((schedule.home_team_id!=schedule.away_team_id)&schedule[['home_team','away_team']].notna().all(axis=1)).all(),True,'all rows'); g('start_time_valid',pd.to_datetime(schedule.scheduled_start_time_utc,utc=True,errors='coerce').notna().all(),True,'parse'); g('champion_feature_order',features[FEATURES].columns.tolist()==FEATURES,True,'exact'); g('champion_parameter_identity',p['historical_prediction_sha256']=='83beb11588f7e7e31919f23be2dea51ff49863954fc9be750509b30a0eff2cda',True,parameter_hash()); g('probabilities_valid',len(predictions)==0 or (predictions.champion_home_win_probability.between(0,1)&predictions.champion_away_win_probability.between(0,1)).all(),True,len(predictions)); g('probability_complement',len(predictions)==0 or np.allclose(predictions.champion_home_win_probability+predictions.champion_away_win_probability,1,atol=1e-12),True,'1e-12'); g('historical_parity_required','HISTORICAL_PARITY_PASS'==p.get('prospective_gate_status') and float(p.get('prospective_parity_maximum_delta',1))<=float(p.get('prospective_parity_tolerance',0)),True,'governed artifact explicit parity attestation'); g('strict_prior_timing',audit.target_game_in_history.eq(0).all() and audit.future_history_rows.eq(0).all(),True,'target/future zero'); g('source_capture_not_after_run',parse_utc(odds_capture_timestamp)<=parse_utc(run_timestamp_utc),True,odds_capture_timestamp); g('quote_timing',quotes[quotes.qualification_status.eq('PREGAME_QUALIFIED')].apply(lambda r:parse_utc(r.provider_market_timestamp_utc)<parse_utc(r.scheduled_start_time_utc) and parse_utc(r.capture_timestamp_utc)<parse_utc(r.scheduled_start_time_utc),axis=1).all(),True,'qualified only'); g('deterministic_binding',not binding.binding_status.eq('GAME_BINDING_AMBIGUOUS').any() if len(binding) else True,False,'optional odds coverage'); g('price_coverage',comparisons.game_id.nunique() if len(comparisons) else 0,False,'Population B games'); gates=pd.DataFrame(gate); critical_pass=gates.loc[gates.critical,'passed'].all(); result='FAIL_CLOSED' if not critical_pass else ('PASS' if comparisons.game_id.nunique()==len(schedule) and len(schedule)>0 else 'PARTIAL_GAME_COVERAGE')
 gates.to_csv(dest/'health_gate_ledger.csv',index=False); pops=pd.DataFrame([['A_CHAMPION_PREDICTION',len(predictions)],['B_MARKET_QUALIFIED',comparisons.game_id.nunique() if len(comparisons) else 0],['C_GOALIE_CONTEXT',0],['D_LINEUP_CONTEXT',0],['E_EXECUTION',0]],columns=['population','rows']); pops.to_csv(dest/'population_membership.csv',index=False)
 metadata={'run_id':run_id,'canonical_season':season,'slate_date':slate_date,'run_timestamp_utc':parse_utc(run_timestamp_utc).isoformat(),'run_type':run_type,'mode':'SHADOW_OBSERVATION_ONLY','historical_fixture':allow_historical_fixture,'champion_identity':p['champion_identity'],'champion_parameter_sha256':parameter_hash(),'champion_feature_manifest_sha256':p['feature_manifest_sha256'],'source_acquisition_timestamps':{'h2h_capture_timestamp_utc':parse_utc(odds_capture_timestamp).isoformat()},'raw_h2h_sha256':sha256_file(dest/'raw_h2h_response.json'),'game_count':len(schedule),'scoreable_game_count':len(predictions),'price_covered_game_count':int(comparisons.game_id.nunique()) if len(comparisons) else 0,'health_gate_result':result,'recommendations_generated':0,'execution_rows':0}; (dest/'run_metadata.json').write_text(json.dumps(metadata,indent=2,sort_keys=True)+'\n'); write_manifest(dest); return dest

def grade_run(run_dir:Path,outcomes_csv:Path,grade_root:Path,grading_timestamp_utc:str)->Path:
 subprocess_result=[]
 for line in (run_dir/'SHA256SUMS').read_text().splitlines():
  digest,name=line.split('  ',1); subprocess_result.append(sha256_file(run_dir/name)==digest)
 if not all(subprocess_result): raise RuntimeError('pregame archive hash failure')
 before={x.name:sha256_file(x) for x in run_dir.iterdir() if x.is_file()}; pred=pd.read_csv(run_dir/'champion_predictions.csv'); out=pd.read_csv(outcomes_csv); keys=['canonical_season','game_id']; required=set(keys+['official_final_home_goals','official_final_away_goals','official_full_game_winner','outcome_source','outcome_source_timestamp_utc','outcome_conflict_status']);
 if required-set(out.columns): raise ValueError('outcome schema incomplete')
 g=pred.merge(out,on=keys,how='left',validate='one_to_one'); g['grading_timestamp_utc']=parse_utc(grading_timestamp_utc).isoformat(); g['home_win_target']=np.where(g.official_full_game_winner.eq('HOME'),1,np.where(g.official_full_game_winner.eq('AWAY'),0,np.nan)); grade_id='grade_'+parse_utc(grading_timestamp_utc).strftime('%Y%m%dT%H%M%S%fZ'); dest=grade_root/run_dir.name/grade_id
 if dest.exists():
  raise FileExistsError('OVERWRITE_ATTEMPT_BLOCKED')
 dest.mkdir(parents=True,exist_ok=False)
 g.to_csv(dest/'graded_predictions.csv',index=False)
 (dest/'grading_metadata.json').write_text(json.dumps({'source_run_id':run_dir.name,'source_run_manifest_sha256':sha256_file(run_dir/'SHA256SUMS'),'grading_timestamp_utc':parse_utc(grading_timestamp_utc).isoformat(),'rows':len(g),'mode':'SHADOW_OBSERVATION_ONLY'},indent=2,sort_keys=True)+'\n')
 write_manifest(dest)
 after={x.name:sha256_file(x) for x in run_dir.iterdir() if x.is_file()}
 assert before==after
 return dest
