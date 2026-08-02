#!/usr/bin/env python3
"""Atomic fixed-time clean-room BetOnline TB 1.5 H1 cohort lifecycle."""
from __future__ import annotations
import argparse,csv,hashlib,io,json,os,shutil,subprocess,sys,tempfile
from collections import Counter
from datetime import date,datetime,time,timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import requests

from backend.mlb.scripts.cleanroom_v1.outcome_lineage import american_profit,reconstruct_stats

ROOT=Path(__file__).resolve().parents[4]
EXPORT_ROOT=ROOT/'backend/mlb/exports/cleanroom_v1/bol_tb15'
COHORT_ROOT=EXPORT_ROOT/'fixed_cohorts'
EVIDENCE_ROOT=ROOT/'artifacts/analysis/model_development/mlb_cleanroom_bol_tb15_daily_lifecycle_certification'
PYTHON=ROOT/'.venv/bin/python'
PT=ZoneInfo('America/Los_Angeles')
CONTRACT='MLB_CLEANROOM_BOL_TB15_FIXED_COHORT_V1'; BLOCK='MLB_CLEANROOM_H1_FIXED_COHORT_BLOCK_V1'
WINDOW_START=time(12,45); WINDOW_END=time(13,15); MIN_GAME=15; MAX_GAME=180
FIELDS=['slate_date','game_pk','player_mlb_id','player','team','opponent','scheduled_first_pitch_utc','capture_timestamp_utc','minutes_until_first_pitch','provider_event_id','governing_run_tag','batting_position','lineup_observation_timestamp','lineup_ingestion_run','over_odds','under_odds','market_observation_timestamp','market_ingestion_run','market_source_payload','market_source_sha256','lineup_source_sha256','population_membership','membership_reason']

def parse(v): return datetime.fromisoformat(str(v).replace('Z','+00:00').replace(' ','T'))
def csv_data(fields,rows):
 s=io.StringIO();w=csv.DictWriter(s,fieldnames=fields,lineterminator='\n');w.writeheader();w.writerows(rows);return s.getvalue().encode()
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def guard(slate:str,at:datetime|None=None):
 now=(at or datetime.now(timezone.utc)).astimezone(PT); requested=date.fromisoformat(slate)
 if requested!=now.date(): raise RuntimeError(f'CAPTURE_DATE_MISMATCH requested_date={requested} current_local_date={now.date()}')
 if not WINDOW_START<=now.time().replace(tzinfo=None)<=WINDOW_END:
  raise RuntimeError(f'FIXED_COHORT_CAPTURE_WINDOW_CLOSED current Pacific timestamp={now.isoformat()} allowed start={WINDOW_START} allowed end={WINDOW_END}')
 return now
def game_in_window(minutes:float)->bool:return MIN_GAME<=minutes<=MAX_GAME
def eligible_lineup(status,pos,observed,capture,pitch):
 if status!='CONFIRMED':return 'LINEUP_NOT_CONFIRMED'
 if not str(pos).isdigit() or not 1<=int(pos)<=9:return 'LINEUP_NOT_CONFIRMED'
 if observed>=pitch:return 'LINEUP_POST_FIRST_PITCH'
 if observed>capture:return 'LINEUP_AFTER_CAPTURE'
 return 'ELIGIBLE'
def market_time_reason(observed,capture,pitch):
 if observed>capture:return 'MARKET_AFTER_CAPTURE'
 if observed>=pitch:return 'MARKET_POST_FIRST_PITCH'
 return 'ELIGIBLE'
def rows(path):
 with path.open(newline='') as f:return list(csv.DictReader(f))

def build(snapshot:Path,schedule_path:Path,binding_path:Path,capture:datetime):
 run=json.loads((snapshot/'run_manifest.json').read_text()); tag=run['run_tag']; slate=run['slate_date']
 schedule=json.loads(schedule_path.read_text()); games={int(g['gamePk']):g for d in schedule.get('dates',[]) for g in d.get('games',[]) if g.get('officialDate')==slate}
 binding_rows=rows(binding_path);bindings={int(r['game_pk']):r for r in binding_rows if r['decision']=='EXACT_UNIQUE_MATCH'};binding_decisions={int(r['game_pk']):r['decision'] for r in binding_rows if r.get('game_pk')}
 lineups={(int(r['game_pk']),int(r['player_mlb_id'])):r for r in rows(snapshot/'lineup_snapshot.csv')}
 identities={(int(r['game_pk']),int(r['player_mlb_id'])):r for r in rows(snapshot/'identity_audit.csv') if r['decision']=='EXACT_UNIQUE_MATCH'}
 hashrows=rows(snapshot/'source_hash_manifest.csv'); hashes={r['sha256']:r['raw_payload_path'] for r in hashrows}
 for r in hashrows:
  p=Path(r['raw_payload_path'])
  if not p.exists() or sha(p)!=r['sha256']:raise RuntimeError('SOURCE_HASH_FAILURE')
 baseline=[]; exclusions=[]; inside=set()
 for game_pk,g in games.items():
  pitch=parse(g['gameDate']); mins=(pitch-capture).total_seconds()/60
  if game_in_window(mins):inside.add(game_pk)
  else:exclusions.append({'game_pk':game_pk,'player_mlb_id':'','player':'','reason':'GAME_OUTSIDE_FIXED_WINDOW','detail':f'{mins:.6f} minutes'})
 two_sided=rows(snapshot/'bol_tb15_two_sided_markets.csv');two_keys={(int(r['game_pk']),int(r['player_mlb_id'])) for r in two_sided}
 seen_one_sided=set()
 for side in rows(snapshot/'bol_tb15_market_sides.csv'):
  key=(int(side['game_pk']),int(side['player_mlb_id']))
  if key[0] in inside and key not in two_keys and key not in seen_one_sided:
   exclusions.append({'game_pk':key[0],'player_mlb_id':key[1],'player':side['player'],'reason':'MARKET_NOT_TWO_SIDED','detail':''});seen_one_sided.add(key)
 for m in two_sided:
  game_pk=int(m['game_pk']);pid=int(m['player_mlb_id'])
  if game_pk not in inside:continue
  g=games[game_pk];pitch=parse(g['gameDate']); reason='ELIGIBLE'
  binding=bindings.get(game_pk); identity=identities.get((game_pk,pid)); lineup=lineups.get((game_pk,pid))
  if not binding:reason='EVENT_BINDING_AMBIGUOUS' if 'AMBIG' in binding_decisions.get(game_pk,'') else 'PROVIDER_EVENT_MISSING'
  elif not identity:reason='PLAYER_IDENTITY_UNRESOLVED'
  elif not lineup:reason='LINEUP_NOT_CONFIRMED'
  else:
   reason=eligible_lineup(lineup['lineup_status'],lineup['batting_order_position'],parse(lineup['snapshot_timestamp_utc']),capture,pitch)
   if reason=='ELIGIBLE':reason=market_time_reason(parse(m['market_timestamp_utc']),capture,pitch)
  if reason!='ELIGIBLE': exclusions.append({'game_pk':game_pk,'player_mlb_id':pid,'player':m['player'],'reason':reason,'detail':''});continue
  away=g['teams']['away']['team'];home=g['teams']['home']['team'];team_id=int(lineup['team_mlb_id'])
  team=away['name'] if team_id==int(away['id']) else home['name'] if team_id==int(home['id']) else ''
  opponent=home['name'] if team_id==int(away['id']) else away['name'] if team_id==int(home['id']) else ''
  if not team:exclusions.append({'game_pk':game_pk,'player_mlb_id':pid,'player':m['player'],'reason':'PLAYER_IDENTITY_UNRESOLVED','detail':'team identity'});continue
  raw_sha=identity['raw_payload_sha256']
  baseline.append({'slate_date':slate,'game_pk':game_pk,'player_mlb_id':pid,'player':m['player'],'team':team,'opponent':opponent,'scheduled_first_pitch_utc':pitch.isoformat(),'capture_timestamp_utc':capture.isoformat(),'minutes_until_first_pitch':(pitch-capture).total_seconds()/60,'provider_event_id':binding['provider_event_id'],'governing_run_tag':tag,'batting_position':int(lineup['batting_order_position']),'lineup_observation_timestamp':lineup['snapshot_timestamp_utc'],'lineup_ingestion_run':lineup['ingestion_run_id'],'over_odds':int(m['over_odds']),'under_odds':int(m['under_odds']),'market_observation_timestamp':m['market_timestamp_utc'],'market_ingestion_run':run['ingestion_run_id'],'market_source_payload':hashes.get(raw_sha,''),'market_source_sha256':raw_sha,'lineup_source_sha256':lineup['source_payload_sha256'],'population_membership':'FIXED_COHORT_BASELINE','membership_reason':'SAME_RUN_FIXED_WINDOW_CONFIRMED_ORDER_TWO_SIDED_EXACT_ID'})
 baseline.sort(key=lambda r:(r['game_pk'],r['player_mlb_id']))
 rejected=[{**r,'population_membership':'FIXED_COHORT_REJECTED_TOP_ORDER'} for r in baseline if r['batting_position']<=3]
 retained=[{**r,'population_membership':'FIXED_COHORT_RETAINED_LOWER_ORDER'} for r in baseline if r['batting_position']>=4]
 return baseline,rejected,retained,exclusions,{'games_examined':len(games),'games_inside_fixed_window':len(inside),**run}

def capture(slate:str,at:datetime|None=None):
 out=COHORT_ROOT/slate; manifest=out/'fixed_cohort_manifest.json'
 if manifest.exists():return {**json.loads(manifest.read_text()),'status':'FIXED_COHORT_ALREADY_FROZEN'}
 now=guard(slate,at); before=set((EXPORT_ROOT/slate/'snapshots').iterdir()) if (EXPORT_ROOT/slate/'snapshots').exists() else set()
 subprocess.run([str(PYTHON),'-u','-m','backend.mlb.scripts.cleanroom_v1.run_cleanroom_bol_tb15_capture','--date',slate],cwd=ROOT,check=True)
 after=set((EXPORT_ROOT/slate/'snapshots').iterdir());new=sorted(after-before)
 if len(new)!=1:raise RuntimeError('fixed cohort could not identify one governing snapshot')
 snapshot=new[0];tag=snapshot.name;pilot=EVIDENCE_ROOT/slate/'runs'/tag
 admitted_capture=parse(json.loads((snapshot/'run_manifest.json').read_text())['capture_timestamp_utc'])
 baseline,rejected,retained,exclusions,counts=build(snapshot,pilot/'raw/MLB_STATS_API'/f'schedule_{slate}.json',pilot/'provider_event_to_game_pk_audit.csv',admitted_capture)
 parent=out.parent;parent.mkdir(parents=True,exist_ok=True);stage=Path(tempfile.mkdtemp(dir=parent,prefix=f'.{slate}_fixed_'))
 try:
  (stage/'raw').mkdir();shutil.copytree(snapshot,stage/'snapshot')
  for h in rows(snapshot/'source_hash_manifest.csv'):
   p=Path(h['raw_payload_path']); shutil.copy2(p,stage/'raw'/f"{h['sha256']}_{p.name}")
  outputs={'fixed_cohort_baseline.csv':(FIELDS,baseline),'fixed_cohort_rejected_top_order.csv':(FIELDS,rejected),'fixed_cohort_retained_lower_order.csv':(FIELDS,retained),'fixed_cohort_exclusions.csv':(['game_pk','player_mlb_id','player','reason','detail'],exclusions)}
  hashes={}
  for name,(fields,data) in outputs.items():(stage/name).write_bytes(csv_data(fields,data));hashes[name]=sha(stage/name)
  status='EMPTY_ELIGIBLE_COHORT' if not baseline else 'FIXED_COHORT_FROZEN'
  m={'contract_name':CONTRACT,'contract_version':1,'research_block':BLOCK,'date':slate,'capture_timestamp_utc':counts['capture_timestamp_utc'],'execution_window_decision':'WITHIN_1245_1315_PT','source_ingestion_ids':[counts['ingestion_run_id']],'games_examined':counts['games_examined'],'games_inside_fixed_window':counts['games_inside_fixed_window'],'provider_events':counts['provider_events'],'raw_market_sides':counts['raw_odds_sides'],'two_sided_markets':counts['two_sided_markets'],'identity_admissions':counts['exact_id_admitted_sides']//2,'identity_rejects':counts['identity_rejects'],'confirmed_lineup_rows':counts['batting_order_rows'],'baseline_identities':len(baseline),'top_order_rejected_identities':len(rejected),'lower_order_retained_identities':len(retained),'exclusions_by_reason':dict(Counter(r['reason'] for r in exclusions)),'source_hashes':{r['raw_payload_path']:r['sha256'] for r in rows(snapshot/'source_hash_manifest.csv')},'output_hashes':hashes,'status':status,'attempt_designation':'POST_HARDENING_FIXED_COHORT_ATTEMPT_001','outcome_fields_present':False}
  (stage/'fixed_cohort_manifest.json').write_text(json.dumps(m,indent=2)+'\n');(stage/'fixed_cohort_capture_report.md').write_text(f'# Fixed cohort — {slate}\n\nStatus: `{status}`\n\nBaseline: {len(baseline)}; rejected 1–3: {len(rejected)}; retained 4–9: {len(retained)}.\n')
  os.replace(stage,out);return m
 finally:
  if stage.exists():shutil.rmtree(stage)

def summarize(graded,pop):
 rr=[r for r in graded if r['population_membership']==pop]; settled=[r for r in rr if r['settlement_status']=='SETTLED'];wins=sum(r['outcome']=='UNDER_WIN' for r in settled);losses=len(settled)-wins;gross=sum(american_profit(5,int(r['under_odds']),True) for r in settled if r['outcome']=='UNDER_WIN');net=gross-losses*5;stake=len(settled)*5
 return {'population':pop,'frozen_rows':len(rr),'actionable_wagers':len(settled),'games_represented':len({r['game_pk'] for r in rr}),'wins':wins,'losses':losses,'no_action':sum(r['settlement_status']=='VOID' for r in rr),'pending':sum(r['settlement_status']=='PENDING' for r in rr),'technical_unresolved':sum(r['settlement_status']=='UNRESOLVED' for r in rr),'win_rate':wins/len(settled) if settled else None,'average_under_odds':sum(int(r['under_odds']) for r in settled)/len(settled) if settled else None,'stake':stake,'net_dollars':net,'roi':net/stake if stake else None}

def closeout(slate:str):
 root=COHORT_ROOT/slate;manifest_path=root/'fixed_cohort_manifest.json'
 if not manifest_path.exists():raise RuntimeError('FIXED_COHORT_FREEZE_REQUIRED')
 frozen=[]
 for pop,file in [('FIXED_COHORT_BASELINE','fixed_cohort_baseline.csv'),('FIXED_COHORT_REJECTED_TOP_ORDER','fixed_cohort_rejected_top_order.csv'),('FIXED_COHORT_RETAINED_LOWER_ORDER','fixed_cohort_retained_lower_order.csv')]:
  frozen += [{**r,'population_membership':pop} for r in rows(root/file)]
 outcomes={};raw=root/'outcome_sources';raw.mkdir(exist_ok=True)
 for game_pk in sorted({int(r['game_pk']) for r in frozen}):
  response=requests.get(f'https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live',timeout=45);response.raise_for_status();digest=hashlib.sha256(response.content).hexdigest();p=raw/f'game_{game_pk}_{digest}.json'
  if not p.exists():p.write_bytes(response.content)
  elif p.read_bytes()!=response.content:raise RuntimeError('OUTCOME_SOURCE_HASH_COLLISION')
  d=response.json();final=d['gameData']['status']['abstractGameState']=='Final';outcomes[(game_pk,None)]={'final':final,'path':str(p),'sha':digest}
  for side in ('away','home'):
   for x in d['liveData']['boxscore']['teams'][side].get('players',{}).values():
    b=(x.get('stats') or {}).get('batting') or {};pid=int(x['person']['id']);h=int(b.get('hits') or 0);db=int(b.get('doubles') or 0);tr=int(b.get('triples') or 0);hr=int(b.get('homeRuns') or 0);dec,singles,tb=reconstruct_stats(h,db,tr,hr);outcomes[(game_pk,pid)]={'final':final,'pa':int(b.get('plateAppearances') or 0),'ab':int(b.get('atBats') or 0),'hits':h,'singles':singles,'doubles':db,'triples':tr,'home_runs':hr,'total_bases':tb,'arithmetic':dec,'path':str(p),'sha':sha(p)}
 graded=[]
 for r in frozen:
  game=int(r['game_pk']);pid=int(r['player_mlb_id']);x=outcomes.get((game,pid));g=outcomes.get((game,None),{})
  if not g.get('final'):outcome,settle='PENDING_GAME','PENDING'
  elif x is None:outcome,settle='TECHNICAL_UNRESOLVED','UNRESOLVED';x={}
  elif x['arithmetic']!='TB_ARITHMETIC_CERTIFIED':outcome,settle='TECHNICAL_UNRESOLVED','UNRESOLVED'
  elif x['pa']==0:outcome,settle='NO_ACTION_ZERO_PLATE_APPEARANCES_OFFICIALLY_SUPPORTED','VOID'
  elif x['total_bases']<=1:outcome,settle='UNDER_WIN','SETTLED'
  else:outcome,settle='UNDER_LOSS','SETTLED'
  graded.append({**r,'plate_appearances':x.get('pa',''),'at_bats':x.get('ab',''),'hits':x.get('hits',''),'singles':x.get('singles',''),'doubles':x.get('doubles',''),'triples':x.get('triples',''),'home_runs':x.get('home_runs',''),'total_bases':x.get('total_bases',''),'outcome':outcome,'settlement_status':settle,'outcome_source':x.get('path',g.get('path','')),'outcome_sha256':x.get('sha',g.get('sha',''))})
 fields=list(graded[0]) if graded else FIELDS+['plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases','outcome','settlement_status','outcome_source','outcome_sha256'];data=csv_data(fields,graded);digest=hashlib.sha256(data).hexdigest();mp=root/'fixed_cohort_closeout_manifest.json';prior=json.loads(mp.read_text()) if mp.exists() else {}
 if prior.get('content_sha256')==digest:return {'status':prior['status'],'revision':prior['revision'],'changed':False}
 revision=int(prior.get('revision',0))+1;summaries=[summarize(graded,p) for p in ('FIXED_COHORT_BASELINE','FIXED_COHORT_REJECTED_TOP_ORDER','FIXED_COHORT_RETAINED_LOWER_ORDER')];base,rej,ret=summaries;impact={'losses_removed':rej['losses'],'winners_sacrificed':rej['wins'],'loss_share_removed':rej['losses']/base['losses'] if base['losses'] else None,'winner_share_removed':rej['wins']/base['wins'] if base['wins'] else None,'removal_advantage':(rej['losses']/base['losses']-rej['wins']/base['wins']) if base['losses'] and base['wins'] else None,'retained_win_rate_change':(ret['win_rate']-base['win_rate']) if ret['win_rate'] is not None and base['win_rate'] is not None else None,'retained_roi_change':(ret['roi']-base['roi']) if ret['roi'] is not None and base['roi'] is not None else None,'largest_game_share_rejected':max(Counter(r['game_pk'] for r in graded if r['population_membership']=='FIXED_COHORT_REJECTED_TOP_ORDER').values(),default=0)/max(rej['frozen_rows'],1)};status='FINAL' if not any(r['settlement_status'] in ('PENDING','UNRESOLVED') for r in graded) else 'OUTCOME_CLOSEOUT_PENDING'
 (root/'fixed_cohort_closeout_rows.csv').write_bytes(data);(root/'fixed_cohort_closeout_summary.json').write_text(json.dumps({'populations':summaries,'h1':impact},indent=2)+'\n');m={'date':slate,'revision':revision,'status':status,'content_sha256':digest,'population_manifest_sha256':sha(manifest_path),'summaries':summaries,'h1':impact};mp.write_text(json.dumps(m,indent=2)+'\n');rev=root/'revisions'/f'revision_{revision:03d}';rev.mkdir(parents=True,exist_ok=False)
 for p in (root/'fixed_cohort_closeout_rows.csv',root/'fixed_cohort_closeout_summary.json',mp):shutil.copy2(p,rev/p.name)
 return {**m,'changed':True}

def window_state(at=None):
 now=(at or datetime.now(timezone.utc)).astimezone(PT);t=now.time().replace(tzinfo=None)
 return 'CAPTURE_WINDOW_NOT_OPEN' if t<WINDOW_START else 'CAPTURE_WINDOW_OPEN' if t<=WINDOW_END else 'NOT_ATTEMPTED'
def status(slate):
 root=COHORT_ROOT/slate;m=json.loads((root/'fixed_cohort_manifest.json').read_text()) if (root/'fixed_cohort_manifest.json').exists() else {};c=json.loads((root/'fixed_cohort_closeout_manifest.json').read_text()) if (root/'fixed_cohort_closeout_manifest.json').exists() else {};base=next((x for x in c.get('summaries',[]) if x['population']=='FIXED_COHORT_BASELINE'),{})
 state=c.get('status') or m.get('status') or window_state();return {'capture_window_state':window_state(),'attempt_status':'ATTEMPTED' if m else 'NOT_ATTEMPTED','freeze_status':m.get('status','NOT_ATTEMPTED'),'capture_timestamp':m.get('capture_timestamp_utc'),'games_inside_window':m.get('games_inside_fixed_window',0),'baseline_rows':m.get('baseline_identities',0),'rejected_top_order_rows':m.get('top_order_rejected_identities',0),'retained_lower_order_rows':m.get('lower_order_retained_identities',0),'exclusion_counts':m.get('exclusions_by_reason',{}),'closeout_revision':c.get('revision',0),'wins':base.get('wins',0),'losses':base.get('losses',0),'no_action':base.get('no_action',0),'pending':base.get('pending',0),'technical_unresolved':base.get('technical_unresolved',0),'net':base.get('net_dollars'),'roi':base.get('roi'),'terminal_date_status':state}
def block_status():
 dates=[]; totals={p:Counter() for p in ('FIXED_COHORT_BASELINE','FIXED_COHORT_REJECTED_TOP_ORDER','FIXED_COHORT_RETAINED_LOWER_ORDER')}; rejected_by_game=Counter()
 for root in sorted(COHORT_ROOT.glob('????-??-??')):
  m=root/'fixed_cohort_manifest.json'
  if not m.exists():continue
  c=json.loads((root/'fixed_cohort_closeout_manifest.json').read_text()) if (root/'fixed_cohort_closeout_manifest.json').exists() else {};final=c.get('status')=='FINAL';base=next((x for x in c.get('summaries',[]) if x['population']=='FIXED_COHORT_BASELINE'),{});rej=next((x for x in c.get('summaries',[]) if x['population']=='FIXED_COHORT_REJECTED_TOP_ORDER'),{});dates.append({'date':root.name,'status':c.get('status','FROZEN'),'baseline_actionable':base.get('actionable_wagers',0) if final else 0,'rejected_actionable':rej.get('actionable_wagers',0) if final else 0})
  if final:
   for x in c.get('summaries',[]):
    for k in ('actionable_wagers','wins','losses','stake','net_dollars','technical_unresolved'):totals[x['population']][k]+=x.get(k,0) or 0
   rp=root/'fixed_cohort_closeout_rows.csv'
   if rp.exists():
    for x in rows(rp):
     if x['population_membership']=='FIXED_COHORT_REJECTED_TOP_ORDER' and x['settlement_status']=='SETTLED':rejected_by_game[(root.name,x['game_pk'])]+=1
 attempts=len(dates);b=totals['FIXED_COHORT_BASELINE']['actionable_wagers'];r=totals['FIXED_COHORT_REJECTED_TOP_ORDER']['actionable_wagers'];complete=(b>=100 and r>=30) or attempts>=5;decision='COLLECTING'
 if complete:
  actionable_dates=sum(x['baseline_actionable']>0 for x in dates);base=totals['FIXED_COHORT_BASELINE'];rej=totals['FIXED_COHORT_REJECTED_TOP_ORDER'];ret=totals['FIXED_COHORT_RETAINED_LOWER_ORDER'];loss_share=rej['losses']/base['losses'] if base['losses'] else 0;win_share=rej['wins']/base['wins'] if base['wins'] else 0;base_wr=base['wins']/b if b else 0;ret_wr=ret['wins']/ret['actionable_wagers'] if ret['actionable_wagers'] else 0;base_roi=base['net_dollars']/base['stake'] if base['stake'] else 0;ret_roi=ret['net_dollars']/ret['stake'] if ret['stake'] else 0;date_share=max((x['rejected_actionable'] for x in dates),default=0)/r if r else 0;game_share=max(rejected_by_game.values(),default=0)/r if r else 0
  if b<100 or r<30 or actionable_dates<3:decision='H1_FIXED_COHORT_INSUFFICIENT_VOLUME_CLOSE_HYPOTHESIS'
  elif loss_share>win_share and ret_wr>base_wr and ret_roi>base_roi and date_share<=.5 and game_share<=.2:decision='H1_FIXED_COHORT_REPLICATED_READY_FOR_OPERATOR_REVIEW'
  else:decision='H1_FIXED_COHORT_FAILED_CLOSE_HYPOTHESIS'
 return {'contract':BLOCK,'attempted_dates':attempts,'actionable_baseline_wagers':b,'actionable_rejected_wagers':r,'stop_rule_met':complete,'collection_state':'BLOCK_COMPLETE' if complete else 'COLLECTING','terminal_decision':decision,'dates':dates}
def main():
 p=argparse.ArgumentParser();p.add_argument('--date');p.add_argument('--mode',choices=('capture','closeout','status','block-status'),required=True);a=p.parse_args()
 if a.mode!='block-status' and not a.date:p.error('--date required')
 result=capture(a.date) if a.mode=='capture' else closeout(a.date) if a.mode=='closeout' else status(a.date) if a.mode=='status' else block_status();print(json.dumps(result,indent=2));return 0
if __name__=='__main__':raise SystemExit(main())
