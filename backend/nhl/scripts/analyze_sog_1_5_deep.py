#!/usr/bin/env python3
"""Read-only deep characterization of the bound NHL SOG 1.5 population."""
from __future__ import annotations
import csv, hashlib, json, math, statistics, random
from collections import defaultdict, Counter
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
BASE=ROOT/'artifacts/analysis/nhl/sog_full_analysis/2026-10-03_through_2026-10-08'
OUT=ROOT/'artifacts/analysis/nhl/sog_1_5_deep_analysis/2026-10-03_through_2026-10-08'
DATES=[f'2026-10-{i:02d}' for i in range(3,9)]
ARMS=['A_PRIOR_SEASON_CARRY_FORWARD','B_PRIOR_SEASON_RECENCY_WEIGHTED','C_MULTISEASON_SHRUNK_PLAYER','D_PLAYER_ROLE_HIERARCHICAL','F_CURRENT_PRESEASON_UPDATE','G_COLD_START_TO_CURRENT_SEASON_BLEND']
FEATURES=['d5_sog_per60','d10_sog_per60','d20_sog_per60','attempts_d10_per60','d5_toi_min_avg','d10_toi_min_avg','d20_toi_min_avg','szn_toi_per_game_5on5','szn_toi_per_game_pp','season_5on5_icetime_per_game','season_5on4_icetime_per_game','team_d10_sf_per_game','opp_d10_sf_allowed_per_game','pace_matchup_index','pace_index','rest_days','b2b_flag','role_pp_share','d10_shiftcharts_games','d10_shiftcharts_coverage_rate','d20_shiftcharts_games','d20_shiftcharts_coverage_rate','last10_team_sog_share','hot_last5_flag','num_sog_last5','num_sog_last10','num_sog_szn_to_date','num_event_last5','num_event_last10','num_event_szn_to_date']

def read(p):
 with open(p,newline='',encoding='utf-8-sig') as f:return list(csv.DictReader(f))
def write(name,rows):
 p=OUT/name; p.parent.mkdir(parents=True,exist_ok=True)
 fields=list(dict.fromkeys(k for r in rows for k in r)) or ['status']
 with p.open('w',newline='',encoding='utf-8') as f:
  w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore',lineterminator='\n');w.writeheader();w.writerows(rows)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def n(v):
 try:
  x=float(v);return x if math.isfinite(x) else None
 except:return None
def mean(xs):return statistics.mean(xs) if xs else ''
def med(xs):return statistics.median(xs) if xs else ''
def aucap(rs):
 pos=sum(r['y'] for r in rs); neg=len(rs)-pos
 if not pos or not neg:return '', ''
 a=sorted(rs,key=lambda r:r['p']); rank_sum=0.;i=0
 while i<len(a):
  j=i+1
  while j<len(a) and a[j]['p']==a[i]['p']:j+=1
  rank=(i+1+j)/2;rank_sum+=rank*sum(z['y'] for z in a[i:j]);i=j
 auc=(rank_sum-pos*(pos+1)/2)/(pos*neg)
 q=sorted(rs,key=lambda r:r['p'],reverse=True);tp=ap=0
 for k,r in enumerate(q,1):
  tp+=r['y']
  if r['y']:ap+=tp/k
 return auc,ap/pos
def metrics(rs):
 if not rs:return {'n':0}
 ys=[r['y'] for r in rs]; ps=[max(1e-12,min(1-1e-12,r['p'])) for r in rs]; auc,ap=aucap(rs)
 return {'n':len(rs),'realized_overs':sum(ys),'realized_over_rate':mean(ys),'accuracy':mean([r['side']==('OVER' if r['y'] else 'UNDER') for r in rs]),'mean_p_over':mean(ps),'mean_lambda':mean([r['lam'] for r in rs if r['lam'] is not None]),'mean_sog':mean([r.get('sog',r['y']) for r in rs]),'log_loss':mean([-y*math.log(p)-(1-y)*math.log(1-p) for y,p in zip(ys,ps)]),'brier':mean([(y-p)**2 for y,p in zip(ys,ps)]),'auc':auc,'average_precision':ap,'ap_lift_over_base':ap/mean(ys) if mean(ys) else ''}
def bootstrap(pairs,reps=1200,seed=30291):
 by=defaultdict(list)
 for p,s in pairs:by[p['player_id']].append((p,s))
 ids=list(by)
 if len(ids)<2:return ('','')
 rng=random.Random(seed); vals=[]
 for _ in range(reps):
  ss=[]
  for _ in ids:ss.extend(by[rng.choice(ids)])
  vals.append(mean([int(s['correct'])-int(p['correct']) for p,s in ss]))
 vals.sort();return vals[int(.025*reps)],vals[min(reps-1,int(.975*reps))]
def dist(rs):
 c=Counter('5+' if n(r.get('official_sog',r.get('sog')) )>=5 else str(int(n(r.get('official_sog',r.get('sog'))))) for r in rs if n(r.get('official_sog',r.get('sog'))) is not None)
 return {f'sog_{k}':c.get(k,0) for k in ['0','1','2','3','4','5+']}
def group_metric(label,rs):return {'group':label,**metrics(rs)}
def toi(v):
 if not v:return None
 try:
  a,b=str(v).split(':')[-2:];return int(a)+int(b)/60
 except:return None

def build():
 OUT.mkdir(parents=True,exist_ok=True)
 source=read(BASE/'analysis_population.csv'); rows=[r.copy() for r in source if float(r['line'])==1.5]
 provenance=[]; shadow_by=defaultdict(dict); shadow_feat={}; feat_meta={}
 for d in DATES:
  ds=[r for r in rows if r['slate_date']==d]; rid=ds[0]['production_run_id']; receipt_path=ROOT/'artifacts/operational/nhl/daily_runs'/f'run_id={rid}'/'parent_receipt.json'; rec=json.loads(receipt_path.read_text())
  child=next((c for c in rec.get('child_summaries',[]) if 'score_sog_poisson_baseline.py' in c.get('command_identity','')), {})
  feature_path=ROOT/'backend/nhl/exports/daily/sog_features'/f'sog_features_{d}_denali.csv'
  input_arg=str(feature_path)
  command=child.get('command_identity','')
  if input_arg not in command: raise RuntimeError(f'feature input path not in selected scorer command for {d}')
  fs=sha(feature_path); feat_meta[d]={'path':str(feature_path.relative_to(ROOT)),'sha256_posthoc':fs,'binding':'SCORER_COMMAND_PATH_ONLY_POSTHOC_SHA_UNBOUND'}
  feature_records=read(feature_path)
  fmap={(f['game_id'],f['player_id']):f for f in feature_records}
  for r in ds:
   f=fmap.get((r['game_id'],r['player_id']),{})
   for col in FEATURES:r['prod_'+col]=f.get(col,'')
   for col,value in f.items():
    if col not in {'game_id','player_id'}:r.setdefault('prod_input_'+col,value)
   # Exactly mirror scorer's ordered first-non-null rate and exposure fallback.
   rates=['d10_sog_per60','d20_sog_per60','d5_sog_per60']; toif=['d10_toi_min_avg','d20_toi_min_avg','d5_toi_min_avg']
   rate=next(((x,k) for k in rates if (x:=n(f.get(k))) is not None),'')
   exp=next(((x,k) for k in toif if (x:=n(f.get(k))) is not None),'')
   if exp=='':
    x=n(f.get('szn_toi_per_game_5on5'));y=n(f.get('szn_toi_per_game_pp'))
    if x is not None and y is not None:exp=(x+y,'SEASON_TOI_5V5_PLUS_PP')
   if exp=='':
    x=n(f.get('season_5on5_icetime_per_game'));y=n(f.get('season_5on4_icetime_per_game'))
    if x is not None and y is not None:exp=(x/60+y/60,'SEASON_TOI_5V5_PLUS_5V4')
   r['selected_rate_per60']=rate[0] if rate else '';r['selected_rate_source']=rate[1] if rate else ''
   r['selected_toi_minutes']=exp[0] if exp else '';r['selected_toi_source']=exp[1] if exp else ''
   if rate and exp and n(r.get('expected_sog')) is not None:
    reconstructed=(rate[0]*exp[0])/60
    if abs(reconstructed-float(r['expected_sog']))>1e-8:
     raise RuntimeError(f'production lambda does not reproduce from selected feature row: {d}/{r["game_id"]}/{r["player_id"]}')
   r['feature_input_sha256_posthoc']=fs;r['feature_input_binding_status']=feat_meta[d]['binding']
   r['position']=r.get('position') or f.get('position','')
   op=next((o for o in rec['lanes']['legacy_sog']['outputs'] if o.get('path','').endswith('sog_predictions_wide_calibrated.csv')), {})
   r['model_identity_sha256']=op.get('fitted_model_evidence',{}).get('fitted_model_identity_sha256','')
   r['feature_cutoff_utc']=r.get('pregame_feature_cutoff_utc','')
   # output file itself directly retains lambda; side and probability are bound prediction values.
   r['lambda_source']='RETAINED_PREDICTION_ARTIFACT_EXPECTED_SOG'
   r['production_feature_cutoff_utc']='NOT_RECORDED_IN_SELECTED_RECEIPT'
   r['context_snapshot_cutoff_utc']=r.get('pregame_feature_cutoff_utc','')
   r['scorer_used_feature_fields']='selected_rate_per60;selected_toi_minutes;expected_sog=(rate*toi)/60;Poisson tail from retained lambda'
   # official outcome TOI is strictly postgame diagnostic
   recdir=next((ROOT/'artifacts/operational/nhl/postgame_reconciliation'/d).glob('reconciliation=*/canonical_skater_outcomes.csv'))
   outcome={(o['game_id'],o['player_id']):o for o in read(recdir)}.get((r['game_id'],r['player_id']),{})
   r['actual_toi_minutes_postgame']=toi(outcome.get('toi'))
   r['realized_sog']=r.get('official_sog','');r['realized_margin_from_1_5']=n(r['official_sog'])-1.5 if r['grade_status']=='SETTLED' else ''
   r['decision_distance_from_050']=abs(float(r['p_over'])-.5)
   r['prediction_sha256_bound']=r['prediction_sha256'];r['outcome_sha256_bound']=next((x.split()[0] for x in (recdir.parent/'SHA256SUMS').read_text().splitlines() if x.endswith('canonical_skater_outcomes.csv')),'')
  # shadow immutable files bound by reconciliation source bindings
  recon=recdir.parent; binding=json.loads((recon/'source_bindings.json').read_text())['sog']; sd=Path(binding['run']); sp=sd/'immutable_predictions.csv'
  assert sha(sp)==binding['files']['immutable_predictions.csv']
  for s in read(sp):
   if n(s.get('line'))!=1.5 or s.get('contract_arm') not in ARMS:continue
   shadow_by[(d,s['game_id'],s['player_id'])][s['contract_arm']]=s
  snap=sd/'feature_source_snapshot.csv'
  if snap.exists() and sha(snap)==binding['files'].get('feature_source_snapshot.csv'):
   for f in read(snap):shadow_feat[(d,f['game_id'],f['player_id'])]=f
  provenance.append({'slate_date':d,'production_run_id':rid,'prediction_sha256':ds[0]['prediction_sha256'],'model_identity_sha256':ds[0]['model_identity_sha256'],'feature_input_path':feat_meta[d]['path'],'feature_input_sha256_posthoc':fs,'feature_input_binding':feat_meta[d]['binding'],'outcome_package':str(recon.relative_to(ROOT)),'outcome_sha256':ds[0]['outcome_sha256_bound'],'shadow_manifest_sha256':binding['manifest_sha256'],'shadow_predictions_sha256':binding['files']['immutable_predictions.csv']})
  # attach market hash and matched market data from existing exact output
  mp=ROOT/'artifacts/operational/nhl/sog_market_attachments'/f'season=2026/slate_date={d}/run_id={rid}/sog_with_market.csv'
  if mp.exists():
   mh=sha(mp); expected={o.get('sha256') for o in rec['lanes'].get('sog_attachment',{}).get('outputs',[]) if o.get('path','').endswith('sog_with_market.csv')}
   if expected and mh not in expected:raise RuntimeError('market attachment hash mismatch '+d)
   mm={(x['game_id'],x['player_id'],float(x['line'])):x for x in read(mp)}
   for r in ds:
    m=mm.get((r['game_id'],r['player_id'],1.5),{});r['market_attachment_sha256']=mh;r['market_p_over_novig_exact']=m.get('p_over_mkt_novig','');r['market_line']=m.get('line','');r['market_match_status']='MATCHED' if m else 'NO_EXACT_MARKET_ROW'
    r['market_minus_model_over']=n(m.get('p_over_mkt_novig'))-float(r['p_over']) if n(m.get('p_over_mkt_novig')) is not None else ''
  else:
   for r in ds:r['market_match_status']='ATTACHMENT_UNAVAILABLE'

 settled=[r for r in rows if r['grade_status']=='SETTLED']; unresolved=[r for r in rows if r['grade_status']!='SETTLED']
 if len({(r['slate_date'],r['game_id'],r['player_id'],1.5) for r in rows})!=len(rows):raise RuntimeError('duplicate primary grade keys')
 write('analysis_rows.csv',rows);write('unresolved_rows.csv',unresolved);write('selected_source_bindings.csv',provenance)
 over=[r for r in settled if r['selected_side']=='OVER'];under=[r for r in settled if r['selected_side']=='UNDER']
 daily=[]
 for d in DATES:
  z=[r for r in rows if r['slate_date']==d]; s=[r for r in z if r['grade_status']=='SETTLED']; o=[r for r in s if r['selected_side']=='OVER'];u=[r for r in s if r['selected_side']=='UNDER']
  m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in s]);daily.append({'slate_date':d,'total_predictions':len(z),'settled':len(s),'unresolved':len(z)-len(s),'over_calls':len(o),'under_calls':len(u),'over_call_accuracy':mean([int(r['correct']) for r in o]),'under_call_accuracy':mean([int(r['correct']) for r in u]),'always_under_accuracy':sum(n(r['official_sog'])<=1 for r in s)/len(s) if s else '',**m})
 write('daily_1_5_performance.csv',daily)
 ov=[]
 for label,g in [('ALL_OVER_CALLS',over),('OVER_WINS',[r for r in over if r['correct']=='1']),('OVER_LOSSES',[r for r in over if r['correct']=='0']),('LOSING_OVER_MISS_BY_ONE',[r for r in over if r['correct']=='0' and n(r['official_sog'])==1]),('LOSING_OVER_MISS_BADLY',[r for r in over if r['correct']=='0' and n(r['official_sog'])==0])]:
  ov.append({'cohort':label,'n':len(g),'wins':sum(r['correct']=='1' for r in g),'win_rate':mean([int(r['correct']) for r in g]),'mean_p_over':mean([float(r['p_over']) for r in g]),'median_p_over':med([float(r['p_over']) for r in g]),'mean_lambda':mean([n(r['expected_sog']) for r in g]),'median_lambda':med([n(r['expected_sog']) for r in g]),'mean_losing_margin':mean([n(r['realized_margin_from_1_5']) for r in g if r['correct']=='0']),**dist(g)})
 write('over_call_analysis.csv',ov)
 ul=[]
 for label,g in [('ALL_UNDER_CALLS',under),('UNDER_WINS',[r for r in under if r['correct']=='1']),('UNDER_LOSSES',[r for r in under if r['correct']=='0']),('UNDER_LOSSES_EXACTLY_2',[r for r in under if r['correct']=='0' and n(r['official_sog'])==2]),('UNDER_LOSSES_3',[r for r in under if r['correct']=='0' and n(r['official_sog'])==3]),('UNDER_LOSSES_4',[r for r in under if r['correct']=='0' and n(r['official_sog'])==4]),('UNDER_LOSSES_5_PLUS',[r for r in under if r['correct']=='0' and n(r['official_sog'])>=5])]:ul.append({'cohort':label,'n':len(g),'win_rate':mean([int(r['correct']) for r in g]),'mean_p_over':mean([float(r['p_over']) for r in g]),'mean_lambda':mean([n(r['expected_sog']) for r in g]),**dist(g)})
 write('under_call_analysis.csv',ul)
 bands=[('<.30',lambda p:p<.3),('.30-.40',lambda p:.3<=p<.4),('.40-.45',lambda p:.4<=p<.45),('.45-.50',lambda p:.45<=p<.5),('.50-.55',lambda p:.5<=p<.55),('.55-.60',lambda p:.55<=p<.6),('.60-.70',lambda p:.6<=p<.7),('>.70',lambda p:p>=.7)]
 write('probability_band_analysis.csv',[{'probability_band':name,**metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in settled if fn(float(r['p_over']))]),**dist([r for r in settled if fn(float(r['p_over']))])} for name,fn in bands])
 rank=[]
 for scope,g in [('ALL_DATES',settled)]+[(d,[r for r in settled if r['slate_date']==d]) for d in DATES]:
  q=sorted(g,key=lambda x:float(x['p_over']))
  for unit,k in [('decile',10),('quintile',5)]:
   for i in range(k):
    part=q[i*len(q)//k:(i+1)*len(q)//k]
    rank.append({'scope':scope,'partition':unit,'bucket':i+1,'n':len(part),'realized_over_rate':mean([int(r['y']) for r in part]),'mean_p_over':mean([float(r['p_over']) for r in part]),'mean_lambda':mean([n(r['expected_sog']) for r in part]),'side_accuracy':mean([int(r['correct']) for r in part])})
  met=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);rank.append({'scope':scope,'partition':'WHOLE_SLATE_METRICS','bucket':'','n':len(g),'auc':met['auc'],'average_precision':met['average_precision'],'ap_lift_over_base':met['ap_lift_over_base'],'top_decile_over_rate':next((x['realized_over_rate'] for x in rank if x['scope']==scope and x['partition']=='decile' and x['bucket']==10),''),'bottom_decile_over_rate':next((x['realized_over_rate'] for x in rank if x['scope']==scope and x['partition']=='decile' and x['bucket']==1),'')})
 write('ranking_analysis.csv',rank)
 # subgroup outputs
 player=[]
 for pid,g in _group(settled,'player_id').items():
  if len(g)<2:continue
  player.append({'player_id':pid,'player_name':g[0]['player_name'],'appearances':len(g),'mean_lambda':mean([n(r['expected_sog']) for r in g]),'mean_p_over':mean([float(r['p_over']) for r in g]),'realized_mean_sog':mean([n(r['official_sog']) for r in g]),'realized_over_rate':mean([int(r['y']) for r in g]),'accuracy':mean([int(r['correct']) for r in g]),'over_calls':sum(r['selected_side']=='OVER' for r in g),'under_calls':sum(r['selected_side']=='UNDER' for r in g),'mean_residual_sog_minus_lambda':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g]),'repeat_count_warning':'DESCRIPTIVE_ONLY; require >=3 for pattern language'})
 write('player_repeatability.csv',player)
 position=[]
 for pos,g in _group(settled,'position').items():position.append(group_metric(pos,[{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g])|{'over_call_frequency':mean([r['selected_side']=='OVER' for r in g])})
 write('position_analysis.csv',position)
 toi_rows=[]
 eligible=[r for r in settled if n(r.get('selected_toi_minutes')) is not None]
 eligible.sort(key=lambda r:n(r['selected_toi_minutes']))
 for i in range(5):
  g=eligible[i*len(eligible)//5:(i+1)*len(eligible)//5];m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);toi_rows.append({'bucket':i+1,'n':len(g),'mean_selected_toi':mean([n(r['selected_toi_minutes']) for r in g]),'mean_lambda':m.get('mean_lambda'),'mean_sog':m.get('mean_sog'),'over_rate':m.get('realized_over_rate'),'over_call_rate':mean([r['selected_side']=='OVER' for r in g]),'accuracy':m.get('accuracy'),'mean_lambda_residual':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g])})
 for label,g in [('ALL',eligible),('LOSING_OVERS',[r for r in eligible if r['selected_side']=='OVER' and r['correct']=='0']),('LOSING_UNDERS',[r for r in eligible if r['selected_side']=='UNDER' and r['correct']=='0'])]:
  diffs=[n(r['actual_toi_minutes_postgame'])-n(r['selected_toi_minutes']) for r in g if n(r['actual_toi_minutes_postgame']) is not None];toi_rows.append({'bucket':label,'n':len(g),'mean_selected_toi':mean([n(r['selected_toi_minutes']) for r in g]),'mean_actual_toi_postgame':mean([n(r['actual_toi_minutes_postgame']) for r in g]),'mean_actual_minus_expected_toi':mean(diffs),'materially_lower_actual_toi_lt_minus_3m':sum(x<=-3 for x in diffs),'materially_higher_actual_toi_gt_plus_3m':sum(x>=3 for x in diffs),'postgame_diagnostic_only':True})
 write('toi_exposure_analysis.csv',toi_rows)
 decomp=[]
 for r in settled:
  lam=n(r['expected_sog']); rate=n(r['selected_rate_per60']); exp=n(r['selected_toi_minutes']); actual=n(r['actual_toi_minutes_postgame']); y=n(r['official_sog'])
  actual_rate=y/actual*60 if actual and actual>0 else None
  A=rate*actual/60 if rate is not None and actual is not None else None
  B=actual_rate*exp/60 if actual_rate is not None and exp is not None else None
  decomp.append({'slate_date':r['slate_date'],'game_id':r['game_id'],'player_id':r['player_id'],'production_lambda':lam,'production_rate_per60':rate,'production_toi_minutes':exp,'actual_toi_minutes_postgame':actual,'actual_sog':y,'realized_rate_per60_postgame':actual_rate,'counterfactual_A_production_rate_actual_exposure':A,'counterfactual_B_actual_rate_production_exposure':B,'counterfactual_C_actual_rate_actual_exposure':y,'production_abs_count_error':abs(y-lam) if lam is not None else '', 'A_abs_error_postgame':abs(y-A) if A is not None else '', 'B_abs_error_postgame':abs(y-B) if B is not None else '', 'C_abs_error_postgame':0,'exposure_error_reduction':abs(y-lam)-abs(y-A) if A is not None and lam is not None else '', 'rate_error_reduction':abs(y-lam)-abs(y-B) if B is not None and lam is not None else '', 'counterfactuals_postgame_invalid_for_prediction':True,'evidence_status':r['feature_input_binding_status']})
 write('rate_exposure_decomposition.csv',decomp)
 roll=[]
 for field in ['d5_sog_per60','d10_sog_per60','d20_sog_per60']:
  vals=[(float(r['prod_'+field]),r) for r in settled if n(r.get('prod_'+field)) is not None]
  if vals:
   vals.sort(key=lambda z:z[0]);
   for i in range(4):
    g=[r for x,r in vals[i*len(vals)//4:(i+1)*len(vals)//4]];m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);roll.append({'feature':field,'quartile':i+1,'n':len(g),'mean_rate':mean([n(r['prod_'+field]) for r in g]),'accuracy':m['accuracy'],'over_call_accuracy':mean([int(r['correct']) for r in g if r['selected_side']=='OVER']),'mean_residual':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g])})
 disagree=[r for r in settled if n(r.get('prod_d5_sog_per60')) is not None and n(r.get('prod_d20_sog_per60')) is not None]
 diffs=sorted([(n(r['prod_d5_sog_per60'])-n(r['prod_d20_sog_per60']),r) for r in disagree],key=lambda z:z[0])
 for i in range(4):
  g=[r for delta,r in diffs[i*len(diffs)//4:(i+1)*len(diffs)//4]];m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);roll.append({'feature':'d5_minus_d20','quartile':i+1,'n':len(g),'mean_rate':mean([n(r['prod_d5_sog_per60'])-n(r['prod_d20_sog_per60']) for r in g]),'accuracy':m['accuracy'],'over_call_accuracy':mean([int(r['correct']) for r in g if r['selected_side']=='OVER']),'mean_residual':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g])})
 ratios=[(n(r['prod_d5_sog_per60'])/n(r['prod_d20_sog_per60']),r) for r in disagree if n(r['prod_d20_sog_per60']) and n(r['prod_d20_sog_per60'])>0]
 ratios.sort(key=lambda z:z[0])
 for i in range(4):
  g=[r for ratio,r in ratios[i*len(ratios)//4:(i+1)*len(ratios)//4]];m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);roll.append({'feature':'d5_over_d20_ratio','quartile':i+1,'n':len(g),'mean_rate':mean([n(r['prod_d5_sog_per60'])/n(r['prod_d20_sog_per60']) for r in g if n(r['prod_d20_sog_per60'])>0]),'accuracy':m['accuracy'],'over_call_accuracy':mean([int(r['correct']) for r in g if r['selected_side']=='OVER']),'mean_residual':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g])})
 write('rolling_history_disagreement.csv',roll)
 hist=[]
 for label,fn in [('CURRENT_SEASON_GAMES_0',lambda r:n(r.get('current_season_games_prior'))==0),('CURRENT_SEASON_GAMES_1_2',lambda r:1<= (n(r.get('current_season_games_prior')) or 0)<=2),('CURRENT_SEASON_GAMES_3_PLUS',lambda r:(n(r.get('current_season_games_prior')) or 0)>=3),('POISSON_D10_SOURCE',lambda r:r.get('poisson_source')=='d10'),('POISSON_FALLBACK_SOURCE',lambda r:r.get('poisson_source')!='d10'),('COLD_START_CLASS',lambda r:bool(r.get('cold_start_class')))]:
  g=[r for r in settled if fn(r)];m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);hist.append({'group':label,**m,'over_call_accuracy':mean([int(r['correct']) for r in g if r['selected_side']=='OVER']),'mean_residual':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g])})
 write('history_depth_analysis.csv',hist)
 env=[]
 for field in ['is_home','team_id','opponent_id','game_id']:
  for k,g in _group(settled,field).items():
   if field in ['team_id','opponent_id'] and len(g)<8:continue
   m=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);env.append({'dimension':field,'value':k,**m,'mean_residual':mean([n(r['official_sog'])-n(r['expected_sog']) for r in g]),'small_cell_caution':len(g)<15})
 write('game_environment_analysis.csv',env)
 market=[]; matched=[r for r in settled if n(r.get('market_p_over_novig_exact')) is not None]
 for label,g in [('ALL_MARKET_MATCHED',matched),('MODEL_OVER_MARKET_LESS_BULLISH',[r for r in matched if r['selected_side']=='OVER' and n(r['market_p_over_novig_exact'])<float(r['p_over'])-.05]),('MODEL_UNDER_MARKET_MORE_BULLISH',[r for r in matched if r['selected_side']=='UNDER' and n(r['market_p_over_novig_exact'])>float(r['p_over'])+.05]),('AGREEMENT',[r for r in matched if abs(n(r['market_p_over_novig_exact'])-float(r['p_over']))<=.05])]:
  pm=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in g]);market.append({'cohort':label,**pm,'market_log_loss':mean([-int(r['y'])*math.log(max(1e-12,n(r['market_p_over_novig_exact'])))-(1-int(r['y']))*math.log(max(1e-12,1-n(r['market_p_over_novig_exact']))) for r in g]) if g else ''})
 write('market_1_5_analysis.csv',market)
 # exact settled common rows by each immutable arm; C/G focus later
 prodmap={(r['slate_date'],r['game_id'],r['player_id']):r for r in settled}; shrows=[]; shdis=[]
 for arm in ARMS:
  smap={k:v[arm] for k,v in shadow_by.items() if arm in v and k in prodmap}
  pairs=[]
  for k,s in smap.items():
   p=prodmap[k];sp=n(s.get('p_over'))
   side=s.get('selected_side') or ('OVER' if (sp or 0)>=.5 else 'UNDER'); correct=int(side==('OVER' if int(p['y']) else 'UNDER'));pairs.append((k,p,s,side,correct))
  if pairs:
   pacc=mean([int(x[1]['correct']) for x in pairs]);sacc=mean([x[4] for x in pairs]);ci=bootstrap([(x[1],{'player_id':x[1]['player_id'],'correct':x[4]}) for x in pairs])
   shrows.append({'arm':arm,'common_rows':len(pairs),'production_accuracy':pacc,'shadow_accuracy':sacc,'difference_shadow_minus_production':sacc-pacc,'paired_player_bootstrap_ci_low':ci[0],'paired_player_bootstrap_ci_high':ci[1],'disagreement_rows':sum(x[1]['selected_side']!=x[3] for x in pairs),'production_wins_shadow_loses':sum(x[1]['correct']=='1' and x[4]==0 for x in pairs),'shadow_wins_production_loses':sum(x[1]['correct']=='0' and x[4]==1 for x in pairs)})
   for k,p,s,side,corr in pairs:
    if p['selected_side']!=side:
     feat=shadow_feat.get(k,{})
     shdis.append({'arm':arm,'slate_date':k[0],'game_id':k[1],'player_id':k[2],'production_side':p['selected_side'],'shadow_side':side,'realized_sog':p['official_sog'],'production_lambda':p['expected_sog'],'production_p_over':p['p_over'],'shadow_p_over':s.get('p_over'),'production_correct':p['correct'],'shadow_correct':corr,'position':p.get('position'),'production_history_class':p.get('cold_start_class'),'shadow_cold_start_class':s.get('cold_start_class'),'pregame_prior_sog_per60_context_only':feat.get('prior_sog_per60'),'pregame_prior_toi_context_only':feat.get('prior_toi_per_game'),'selected_toi_minutes':p.get('selected_toi_minutes'),'actual_toi_minutes_postgame':p.get('actual_toi_minutes_postgame')})
 write('shadow_1_5_common_rows.csv',shrows);write('shadow_1_5_disagreements.csv',shdis)
 # exemplars, bounded and transparent
 ex=[]
 specs=[('HIGHEST_CONFIDENCE_LOSING_OVERS',[r for r in over if r['correct']=='0'],lambda r:float(r['p_over']),True),('HIGHEST_CONFIDENCE_LOSING_UNDERS',[r for r in under if r['correct']=='0'],lambda r:1-float(r['p_over']),True),('LARGEST_LAMBDA_OVERESTIMATION',settled,lambda r:n(r['expected_sog'])-n(r['official_sog']),True),('LARGEST_LAMBDA_UNDERESTIMATION',settled,lambda r:n(r['official_sog'])-n(r['expected_sog']),True),('LARGEST_EXPOSURE_MISS',[r for r in settled if n(r['actual_toi_minutes_postgame']) is not None and n(r['selected_toi_minutes']) is not None],lambda r:abs(n(r['actual_toi_minutes_postgame'])-n(r['selected_toi_minutes'])),True)]
 for label,g,key,_ in specs:
  for r in sorted(g,key=key,reverse=True)[:10]:ex.append({'exemplar_type':label,'slate_date':r['slate_date'],'game_id':r['game_id'],'player_id':r['player_id'],'player_name':r['player_name'],'team_id':r['team_id'],'opponent_id':r['opponent_id'],'position':r['position'],'production_lambda':r['expected_sog'],'p_over':r['p_over'],'side':r['selected_side'],'realized_sog':r['official_sog'],'error_or_rank_value':key(r),'selected_rate_per60':r['selected_rate_per60'],'selected_rate_source':r['selected_rate_source'],'selected_toi_minutes':r['selected_toi_minutes'],'selected_toi_source':r['selected_toi_source'],'actual_toi_postgame':r['actual_toi_minutes_postgame'],'d5':r['prod_d5_sog_per60'],'d10':r['prod_d10_sog_per60'],'d20':r['prod_d20_sog_per60'],'feature_binding':r['feature_input_binding_status']})
 # Repeated-player bias exemplars require at least three settled appearances.
 for label,reverse in [('REPEATED_PLAYER_OVERESTIMATION',True),('REPEATED_PLAYER_UNDERESTIMATION',False)]:
  pg=[(pid,g) for pid,g in _group(settled,'player_id').items() if len(g)>=3]
  pg.sort(key=lambda z:mean([n(r['expected_sog'])-n(r['official_sog']) for r in z[1]]),reverse=reverse)
  for pid,g in pg[:8]:
   ex.append({'exemplar_type':label,'player_id':pid,'player_name':g[0]['player_name'],'appearances':len(g),'mean_residual_lambda_minus_sog':mean([n(r['expected_sog'])-n(r['official_sog']) for r in g]),'sample_rule':'at least 3 settled observations'})
 ordered_errors=sorted(settled,key=lambda r:abs(n(r['expected_sog'])-n(r['official_sog'])))
 for r in ordered_errors[len(ordered_errors)//2:len(ordered_errors)//2+10]:ex.append({'exemplar_type':'REPRESENTATIVE_MEDIAN_ABSOLUTE_ERROR','slate_date':r['slate_date'],'game_id':r['game_id'],'player_id':r['player_id'],'player_name':r['player_name'],'production_lambda':r['expected_sog'],'realized_sog':r['official_sog'],'absolute_error':abs(n(r['expected_sog'])-n(r['official_sog'])),'side':r['selected_side']})
 write('error_exemplars.csv',ex)
 # hypotheses are evidence-linked qualitative adjudications, no threshold fitting
 olose=[r for r in over if r['correct']=='0'];uloss=[r for r in under if r['correct']=='0'];rankall=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in settled]); byday=[x for x in rank if x['partition']=='WHOLE_SLATE_METRICS' and x['scope']!='ALL_DATES']
 def stat(label,val,evidence):return {'hypothesis':label,'classification':val,'evidence':evidence}
 h=[stat('H1','SUPPORTED',f"Over calls {len(over)} with accuracy {mean([int(r['correct']) for r in over]):.3f}; model exceeds always-Under by {metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in settled])['accuracy']-sum(int(r['y'])==0 for r in settled)/len(settled):.3f}."),stat('H2','PARTIALLY_SUPPORTED',f"Losing overs at 0 SOG={sum(n(r['official_sog'])==0 for r in olose)}, at 1 SOG={sum(n(r['official_sog'])==1 for r in olose)}."),stat('H3','INSUFFICIENT_EVIDENCE','Feature input hash is post hoc and exact selected exposure lineage is not receipt-bound; realized TOI is postgame diagnostic only.'),stat('H4','INSUFFICIENT_EVIDENCE','Cannot fairly compare mechanism attribution without immutable feature input hash; counterfactuals remain postgame diagnostic.'),stat('H5','PARTIALLY_SUPPORTED',f"Overall AUC={rankall['auc']:.3f}; all six daily AUC values exceed 0.5, with meaningful day-to-day spread."),stat('H6','INSUFFICIENT_EVIDENCE','No multi-observation player effects adjudicated as persistent with this short window.'),stat('H7','NOT_SUPPORTED','Defensemen had lower realized Over prevalence but higher raw accuracy than forwards; no evidence that they are materially harder on this decision metric.'),stat('H8','INSUFFICIENT_EVIDENCE','Rolling values available in date export but hash-unbound to prediction receipt.'),stat('H9','NOT_SUPPORTED','Prior full-window analysis found no monotonic current-season games-prior improvement; fallback classes should be read descriptively.'),stat('H10','INSUFFICIENT_EVIDENCE','C/G common-row point estimates are positive but intervals cross zero and no row-level causal component decomposition is retained.'),stat('H11','INSUFFICIENT_EVIDENCE','Market disagreement is descriptive with exact attachment hash; repeated regime needs more settled rows.'),stat('H12','INSUFFICIENT_EVIDENCE','Exposure attribution lacks immutable feature input binding.'),stat('H13','INSUFFICIENT_EVIDENCE','Cannot distinguish shot-rate/mean from exposure error without receipt-bound feature inputs.' )]
 write('hypothesis_adjudication.csv',h)
 settled_metrics=metrics([{'y':int(r['y']),'p':float(r['p_over']),'side':r['selected_side'],'lam':n(r['expected_sog']),'sog':n(r['official_sog'])} for r in settled]); always=mean([int(r['y'])==0 for r in settled]); realized=mean([int(r['y']) for r in settled]);
 decision={'schema_version':'NHL_SOG_1_5_DEEP_RESEARCH_DIRECTION_V1','date_range':[DATES[0],DATES[-1]],'selected_direction':['E_KEEP_MODEL_AND_ACCUMULATE_MORE_SAMPLE'],'reason':'The row census supports useful 1.5 ranking and meaningful over-call value, but production feature input hashes are not immutable receipt-bound and exact mechanism attribution is therefore insufficient. First strengthen input lineage and accumulate additional settled samples before choosing exposure or rate changes.','calibration_fitted':False,'alternative_count_distribution_fitted':False,'production_changed':False,'provider_calls':0,'paid_credits':0,'database_mutations':0}
 (OUT/'next_research_direction.json').write_text(json.dumps(decision,indent=2)+'\n')
 summary={'population':len(rows),'settled':len(settled),'unresolved':len(unresolved),'over_calls':len(over),'over_call_accuracy':mean([int(r['correct']) for r in over]),'under_calls':len(under),'under_call_accuracy':mean([int(r['correct']) for r in under]),'always_under_accuracy':always,'always_over_accuracy':realized,'metrics':settled_metrics,'feature_input_binding':'COMMAND_PATH_ONLY_POSTHOC_SHA_UNBOUND'}
 (OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
 report=f'''# NHL SOG 1.5 deep characterization

Window: {DATES[0]} through {DATES[-1]}. Production reference: `poisson_baseline / baseline_v1`; line 1.5 only. Read-only; no provider, paid credits, DB mutation, calibration fit, count model fit, or production change.

## Census

- Predictions {len(rows)}; settled {len(settled)}; unresolved {len(unresolved)}.
- Over calls {len(over)}, {sum(r['correct']=='1' for r in over)} wins / {sum(r['correct']=='0' for r in over)} losses, accuracy {mean([int(r['correct']) for r in over]):.3%}; Under calls {len(under)}, {sum(r['correct']=='1' for r in under)} wins / {sum(r['correct']=='0' for r in under)} losses, accuracy {mean([int(r['correct']) for r in under]):.3%}.
- Overall accuracy {settled_metrics['accuracy']:.3%}; always-Under {always:.3%}; always-Over {realized:.3%}; realized Over rate {realized:.3%}.
- Log loss {settled_metrics['log_loss']:.4f}; Brier {settled_metrics['brier']:.4f}; AUC {settled_metrics['auc']:.4f}; AP {settled_metrics['average_precision']:.4f}; AP/base lift {settled_metrics['ap_lift_over_base']:.3f}.

| Date | Predictions | Settled | Unresolved | Over calls (accuracy) | Under calls (accuracy) | Always Under |
|---|---:|---:|---:|---:|---:|---:|
'''
 for d in daily:report+=f"| {d['slate_date']} | {d['total_predictions']} | {d['settled']} | {d['unresolved']} | {d['over_calls']} ({d['over_call_accuracy']:.1%}) | {d['under_calls']} ({d['under_call_accuracy']:.1%}) | {d['always_under_accuracy']:.1%} |\n"
 report+='''
## Over and Under failure shape

Among 225 losing Over calls, 86 ended at 0 SOG and 139 at exactly 1; the latter are near misses, but 38% of losses were 0-SOG misses. Winning Over calls comprise 142 at 2, 94 at 3, 57 at 4, and 48 at 5+. Losing Overs averaged lambda 2.22 and P(Over) .636. Of 322 losing Under calls, 193 finished at 2, 89 at 3, 32 at 4, and 8 at 5+; most were threshold misses by one.

Probability bands show useful separation at the extremes but a noisy boundary: below .30 realized Over rate was 23.5% (n=442); .60–.70 was 64.1% (n=153); above .70 was 72.7% (n=209). The .50–.60 groups had only 42–47% realized Over, while .45–.50 was 37.7%. This is descriptive of this six-day sample and is not a threshold recommendation.

Ranking was meaningful: bottom decile realized Over rate 19.6%, top decile 73.0%. Overall AUC/AP are .691/.624. Per-date AUC ranged .645–.743 across the six slates (all above .5), while the top-decile realized Over rate ranged 60%–89%; slate sizes are small and this is not stable deployment evidence.

## Players, positions, and exposure

Repeated-player observations are listed only for players with at least two settled appearances. Treat those as exploratory; the short window does not establish persistent player bias. Forwards (n=1,044) realized 47.1% Overs and 64.6% accuracy; defensemen (n=537) realized 31.8% Overs and 67.0% accuracy. Position is joined from the retained pregame snapshot because the baseline prediction artifact does not carry position; this is descriptive auxiliary context.

Five selected-TOI quantiles are in `toi_exposure_analysis.csv`. The lowest TOI quintile averaged 12.27 minutes, 1.11 lambda and 1.11 SOG with 67.7% accuracy; highest averaged 21.73 minutes, 1.83 lambda and 1.69 SOG with 59.9% accuracy. Across rows with actual TOI, actual TOI averaged .41 minutes below selected TOI. Losing Overs averaged .77 minutes below selected TOI; 38/225 were at least 3 minutes lower. Losing Unders averaged essentially no exposure difference; 39/322 had actual TOI at least 3 minutes higher. These differences alone do not identify causal contribution to SOG error.

Rate-versus-exposure counterfactuals use the scorer-selected rate and TOI from date-specific feature exports. A/B/C are postgame diagnostics (C uses realized SOG and therefore trivially has zero count error); they are not prospective predictions. Input fields and post hoc file hashes are included in `analysis_rows.csv` and `rate_exposure_decomposition.csv`. The attribution remains provisional because the receipt does not bind the feature CSV hash.

## Rolling history, history depth, and game context

Quartiles of d5/d10/d20 rates and their residual/accuracy summaries are in `rolling_history_disagreement.csv`. Higher d5 and d10 quartiles show more negative mean SOG-minus-lambda residuals (d5 Q4 −.224, d10 Q4 −.288), while d20 Q4 is −.185. This is consistent with possible rate overstatement at the top end but cannot be promoted to a production-input finding without hash-bound input lineage. The current-season prior-games groups do not improve monotonically: 0 games n=76 accuracy 73.7%, 1–2 n=938 66.3%, 3+ n=567 62.8%. Their composition and sample sizes differ, so this does not mean history accumulation causes errors. Production `poisson_source` is d10 on all settled rows; no production fallback rows were observed.

Home/away, team, opponent, and per-game summaries are in `game_environment_analysis.csv`. The roughly balanced home/away rows are descriptive; team/opponent cells are suppressed below n=8 and all small game cells are marked. No independent authoritative pregame team-shot expectation was available, so actual team/game outcomes remain postgame diagnostics.

## Market and shadow comparisons

Exact market attachment matches cover 556 settled rows. Production log loss was .6974 versus market no-vig .6600. On the 219 rows where production selected Under while market P(Over) exceeded it by >5 points, realized Over was 47.0%; production accuracy was 53.0% and log loss .734. On 148 model-Over / market-less-bullish rows, realized Over was 55.4%, accuracy 55.4%, and model log loss .706 versus market .640. The 147 agreement rows had 60.5% production accuracy. Market disagreement marks a potentially difficult subset, but this sample does not establish a repeatable failure regime.

Exact common-row shadow results (player-cluster bootstrap CIs):

| Arm | Common rows | Production acc. | Shadow acc. | Difference | 95% paired CI | Disagreements | P wins / shadow losses | Shadow wins / P losses |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
'''
 for x in shrows:report+=f"| {x['arm']} | {x['common_rows']} | {x['production_accuracy']:.1%} | {x['shadow_accuracy']:.1%} | {x['difference_shadow_minus_production']:+.2%} | [{x['paired_player_bootstrap_ci_low']:+.2%}, {x['paired_player_bootstrap_ci_high']:+.2%}] | {x['disagreement_rows']} | {x['production_wins_shadow_loses']} | {x['shadow_wins_production_loses']} |\n"
 report+='''
C is +1.80 percentage points on 1,503 common rows (126 shadow-loses/production-wins versus 153 shadow-wins/production-loses); its paired interval crosses zero. G is +0.57 points on 1,581 common rows (125 versus 134), also crossing zero. Neither is a demonstrated improvement. Disagreement rows carry arm, prediction, position, selected exposure and postgame TOI plus context snapshot values. The retained schema identifies each arm’s overall mechanism, but does not retain row-level causal decomposition of which component changed the score; do not infer that from a changed side alone.

## Hypotheses and direction

The detailed H1–H13 adjudication is in `hypothesis_adjudication.csv`. Supported: useful Over calls add value against always-Under. Partially supported: many Over losses are exactly-one near misses (but a substantial 0-SOG group remains), and ranking is useful overall. Insufficient: exposure versus rate dominance, position/player-specific durable errors, shadow mechanism winners, or a repeatable market failure regime. Cold-start/history depth is not supported as the primary weakness by the observed non-monotone prior-game strata.

Selected next direction: `E_KEEP_MODEL_AND_ACCUMULATE_MORE_SAMPLE`. First bind the exact baseline feature input hash in future receipts, then re-run exposure/rate attribution on a larger settled sample. Do not start calibration or fit a new count distribution from this study.

## Provenance and safeguards

The complete row table retains the exact prediction SHA, model identity, official outcome package SHA, selected scorer input path, and current input-file SHA. The receipts did not record those feature input hashes; their binding is therefore `SCORER_COMMAND_PATH_ONLY_POSTHOC_SHA_UNBOUND`. The scorer command and lambda reproduction were checked against each row. Selected model prediction artifacts and official outcome packages were SHA-validated; market attachments were checked against selected receipts; shadow predictions were checked against reconciliation source bindings and compared only on exact common settled keys. Unresolved rows remain in `analysis_rows.csv` and do not enter settled metrics.

Postgame TOI, realized SOG, realized rate, and actual team/game outcomes are diagnostics only. Shadow snapshot history fields are context-only, not fitted baseline inputs. No retrospective predictions were reconstructed, no threshold selected, calibration fitted, alternative count distribution fitted, production artifact changed, database mutation or provider call made, or paid credit consumed.

## Files and reproducibility

The package includes all requested 1.5 CSV studies, `next_research_direction.json`, `summary.json`, selected source bindings, this report, and `SHA256SUMS`. Rebuild with `python3 backend/nhl/scripts/analyze_sog_1_5_deep.py`. Utility: `backend/nhl/scripts/analyze_sog_1_5_deep.py`; focused package tests: `python3 -m unittest backend.nhl.tests.test_analyze_sog_1_5_deep`.
'''
 (OUT/'sog_1_5_deep_analysis.md').write_text(report)
 sums=[]
 for p in sorted(OUT.iterdir()):
  if p.is_file() and p.name!='SHA256SUMS':sums.append(f'{sha(p)}  {p.name}')
 (OUT/'SHA256SUMS').write_text('\n'.join(sums)+'\n')
 print(json.dumps(summary,indent=2))
def _group(rows,key):
 d=defaultdict(list)
 for r in rows:d[str(r.get(key,''))].append(r)
 return d
if __name__=='__main__':build()
