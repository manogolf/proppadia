#!/usr/bin/env python3
"""Certify routine-cohort outcomes against immutable official MLB payloads."""
from __future__ import annotations
import argparse,csv,hashlib,json
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path
import requests
from backend.shared.db.pg import pg_connect
from backend.mlb.scripts.cleanroom_v1.routine_market_lifecycle import role
ROOT=Path(__file__).resolve().parents[4]
ROUTINE=ROOT/'backend/mlb/exports/cleanroom_v1/bol_tb15/routine_cohorts'
AUG2=ROOT/'backend/mlb/exports/cleanroom_v1/bol_tb15/schedule_cohorts/2026-08-02'
EVIDENCE=ROOT/'artifacts/analysis/model_development/mlb_cleanroom_august2_normal_outcome_reconciliation/2026-08-03'
LOCAL_FIELDS=['player_id','game_id','game_date','team','opponent','is_home','position','is_starter','plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases','pa_source','pa_backfilled_at']
def sh(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def read(p):
 with p.open(newline='') as f:return list(csv.DictReader(f))
def write(p,fields,rows):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields,lineterminator='\n');w.writeheader();w.writerows(rows)
def fields_for(rows):return list(dict.fromkeys(k for r in rows for k in r))
def local_rows(game_ids):
 with pg_connect() as conn,conn.cursor() as cur:
  cur.execute('SELECT player_id,game_id,game_date,team,opponent,is_home,position,is_starter,plate_appearances,at_bats,hits,singles,doubles,triples,home_runs,total_bases,pa_source,pa_backfilled_at FROM mlb.player_stats WHERE game_id = ANY(%s)',([int(x) for x in game_ids],))
  return [{k:v for k,v in zip(LOCAL_FIELDS,row)} if not isinstance(row,dict) else dict(row) for row in cur.fetchall()]
def aug2_population():
 baseline=[{**r,'original_partition':'FORMERLY_LINEUP_ADMITTED'} for r in read(AUG2/'schedule_cohort_baseline.csv')]
 two={(r['game_pk'],r['player_mlb_id']):r for r in read(AUG2/'snapshot/bol_tb15_two_sided_markets.csv')};ident={(r['game_pk'],r['player_mlb_id']):r for r in read(AUG2/'snapshot/identity_audit.csv') if r.get('decision')=='EXACT_UNIQUE_MATCH'}
 excluded=[]
 for x in read(AUG2/'schedule_cohort_exclusions.csv'):
  if x.get('reason')!='LINEUP_NOT_CONFIRMED':continue
  key=(x['game_pk'],x['player_mlb_id']);m=two[key];i=ident[key]
  excluded.append({'slate_date':'2026-08-02','game_pk':x['game_pk'],'player_mlb_id':x['player_mlb_id'],'player':x['player'],'team_mlb_id':i.get('team_mlb_id',''),'over_odds':m['over_odds'],'under_odds':m['under_odds'],'original_partition':'FORMERLY_LINEUP_NOT_CONFIRMED'})
 return baseline+excluded
def routine_population(date):return [{**r,'original_partition':'ROUTINE_MARKET_BASELINE'} for r in read(ROUTINE/date/'routine_market_baseline.csv')]
def official_feed(game,out):
 candidates=list((AUG2/'outcome_sources').glob(f'game_{game}_*.json')) if out==EVIDENCE else list((out/'official_sources').glob(f'game_{game}_*.json'))
 for p in candidates:
  d=json.loads(p.read_text())
  if d.get('gameData',{}).get('status',{}).get('abstractGameState')=='Final':return d,p,sh(p),'PRESERVED_OFFICIAL_FEED'
 q=requests.get(f'https://statsapi.mlb.com/api/v1.1/game/{game}/feed/live',timeout=45);q.raise_for_status();h=hashlib.sha256(q.content).hexdigest();p=out/'official_sources'/f'game_{game}_{h}.json';p.parent.mkdir(parents=True,exist_ok=True)
 if not p.exists():p.write_bytes(q.content)
 return q.json(),p,h,'FRESH_OFFICIAL_RECOVERY'
def player_result(feed,pid):
 if feed.get('gameData',{}).get('status',{}).get('abstractGameState')!='Final':return {'official_status':'OFFICIAL_GAME_NOT_FINAL','role':'ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED'}
 player=None;side_name=''
 for side in ('away','home'):
  p=feed['liveData']['boxscore']['teams'][side].get('players',{}).get(f'ID{pid}')
  if p:player=p;side_name=side
 if not player:return {'official_status':'TECHNICAL_UNRESOLVED_NO_EXACT_OFFICIAL_SUPPORT','role':'ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED'}
 rr,pos=role(player);b=(player.get('stats') or {}).get('batting') or {};h=int(b.get('hits') or 0);db=int(b.get('doubles') or 0);tr=int(b.get('triples') or 0);hr=int(b.get('homeRuns') or 0);sing=h-db-tr-hr;tb=sing+2*db+3*tr+4*hr;pa=int(b.get('plateAppearances') or 0)
 if rr=='DID_NOT_APPEAR':status='OFFICIAL_NONAPPEARANCE_SUPPORTED'
 elif rr=='ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED':status='OFFICIAL_ROLE_ONLY_RESULT_MISSING'
 elif pa==0:status='OFFICIAL_ZERO_PA_APPEARANCE_RECOVERED'
 else:status='OFFICIAL_APPEARANCE_RESULT_RECOVERED'
 return {'official_status':status,'role':rr,'final_batting_position':pos,'plate_appearances':pa,'at_bats':int(b.get('atBats') or 0),'hits':h,'singles':sing,'doubles':db,'triples':tr,'home_runs':hr,'total_bases':tb,'team_side':side_name}
def classify_local(rows,official):
 if len(rows)>1:return 'DUPLICATE_LOCAL_PLAYER_STATS_ROWS'
 if not rows:return 'LOCAL_ROW_MISSING'
 local=rows[0]
 if official['official_status'] in ('TECHNICAL_UNRESOLVED_NO_EXACT_OFFICIAL_SUPPORT','OFFICIAL_ROLE_ONLY_RESULT_MISSING'):return 'EXACT_LOCAL_PLAYER_STATS_ROW'
 for k in ('plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases'):
  if local.get(k) is None and k=='plate_appearances':continue
  if int(local.get(k) or 0)!=int(official.get(k) or 0):return 'LOCAL_ROW_STAT_MISMATCH'
 return 'LOCAL_ROW_VERIFIED_EXACT'
def reconcile(date,out=None):
 out=out or (EVIDENCE if date=='2026-08-02' else ROUTINE/date);population=aug2_population() if date=='2026-08-02' else routine_population(date);games=sorted({int(r['game_pk']) for r in population});local=local_rows(games);by=defaultdict(list)
 for x in local:by[(int(x['game_id']),int(x['player_id']))].append(x)
 feeds={g:official_feed(g,out) for g in games};rows=[];corrections=[]
 for r in population:
  game=int(r['game_pk']);pid=int(r['player_mlb_id']);feed,path,digest,acq=feeds[game];official=player_result(feed,pid);lr=by[(game,pid)];decision=classify_local(lr,official);local_one=lr[0] if len(lr)==1 else {}
  final='LOCAL_ROW_VERIFIED_EXACT' if decision=='LOCAL_ROW_VERIFIED_EXACT' else official['official_status'] if decision=='LOCAL_ROW_MISSING' else 'OFFICIAL_SOURCE_GOVERNS_LOCAL_CONFLICT' if decision=='LOCAL_ROW_STAT_MISMATCH' else decision
  row={**r,'normal_player_stats_row_count':len(lr),'local_status':decision,**{f'local_{k}':local_one.get(k,'') for k in LOCAL_FIELDS},'official_game_final':feed.get('gameData',{}).get('status',{}).get('abstractGameState')=='Final','official_source_status':official.get('official_status'),'final_participation_role':official.get('role'),'official_final_batting_position':official.get('final_batting_position',''),**{f'official_{k}':official.get(k,'') for k in ('plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases')},'official_source_payload':str(path.relative_to(ROOT)),'official_source_sha256':digest,'source_observation_timestamp':datetime.fromtimestamp(path.stat().st_mtime,timezone.utc).isoformat(),'source_acquisition':acq,'final_support_decision':final}
  rows.append(row)
  if decision=='LOCAL_ROW_STAT_MISMATCH':
   for k in ('plate_appearances','at_bats','hits','singles','doubles','triples','home_runs','total_bases'):
    if str(local_one.get(k,''))!=str(official.get(k,'')):corrections.append({'slate_date':date,'game_pk':game,'player_mlb_id':pid,'field':k,'old_value':local_one.get(k,''),'corrected_value':official.get(k,''),'source_payload':str(path.relative_to(ROOT)),'source_sha256':digest,'reason':'OFFICIAL_SOURCE_CONFLICT','discovery_timestamp':datetime.now(timezone.utc).isoformat(),'database_write':'NOT_AUTHORIZED'})
 return population,rows,corrections,feeds,local
def summary(rows,corrections):
 return {'frozen_identities':len(rows),'exact_local_player_stats_rows':sum(int(r['normal_player_stats_row_count'])==1 for r in rows),'local_rows_verified_against_official_source':sum(r['local_status']=='LOCAL_ROW_VERIFIED_EXACT' for r in rows),'local_rows_corrected_by_official_source':len({(r['game_pk'],r['player_mlb_id']) for r in rows if r['local_status']=='LOCAL_ROW_STAT_MISMATCH'}),'officially_recovered_missing_rows':sum(r['local_status']=='LOCAL_ROW_MISSING' and r['final_support_decision'].startswith('OFFICIAL_') for r in rows),'supported_nonappearances':sum(r['official_source_status']=='OFFICIAL_NONAPPEARANCE_SUPPORTED' for r in rows),'technical_unresolved':sum('UNRESOLVED' in r['final_support_decision'] or r['final_support_decision']=='OFFICIAL_ROLE_ONLY_RESULT_MISSING' for r in rows),'stat_conflicts':sum(r['local_status']=='LOCAL_ROW_STAT_MISMATCH' for r in rows),'identity_conflicts':sum(r['local_status']=='DUPLICATE_LOCAL_PLAYER_STATS_ROWS' for r in rows),'source_payload_coverage':sum(bool(r['official_source_sha256']) for r in rows),'correction_fields':len(corrections)}
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--date',required=True);a=ap.parse_args();out=EVIDENCE if a.date=='2026-08-02' else ROUTINE/a.date;pop,rows,corr,feeds,local=reconcile(a.date,out);write(out/'certified_outcome_reconciliation.csv',fields_for(rows),rows);write(out/'correction_overlay_manifest.csv',fields_for(corr) if corr else ['slate_date','game_pk','player_mlb_id','field','old_value','corrected_value','source_payload','source_sha256','reason','discovery_timestamp','database_write'],corr);s=summary(rows,corr);(out/'certified_outcome_reconciliation_summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))
if __name__=='__main__':main()
